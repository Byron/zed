use anyhow::{Context as _, Result};
use collections::HashMap;
use cpal::{
    DeviceDescription, DeviceId, default_host,
    traits::{DeviceTrait, HostTrait},
};
use gpui::{App, AsyncApp, BorrowAppContext, Global, Task};

pub(super) use cpal::Sample;

use rodio::{
    Decoder, DeviceSinkBuilder, MixerDeviceSink, Source,
    mixer::Mixer,
    source::{Buffered, Done},
};
use settings::Settings;
use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use util::ResultExt;

mod echo_canceller;
use echo_canceller::EchoCanceller;
mod rodio_ext;
pub use crate::audio_settings::AudioSettings;
pub use rodio_ext::RodioExt;

use crate::Sound;

use super::{CHANNEL_COUNT, SAMPLE_RATE};
pub const BUFFER_SIZE: usize = // echo canceller and livekit want 10ms of audio
    (SAMPLE_RATE.get() as usize / 100) * CHANNEL_COUNT.get() as usize;

pub fn init(_cx: &mut App) {}

// TODO(jk): this is currently cached only once - we should observe and react instead
pub fn ensure_devices_initialized(cx: &mut App) {
    if cx.has_global::<AvailableAudioDevices>() {
        return;
    }
    cx.default_global::<AvailableAudioDevices>();
    let task = cx
        .background_executor()
        .spawn(async move { get_available_audio_devices() });
    cx.spawn(async move |cx: &mut AsyncApp| {
        let devices = task.await;
        cx.update(|cx| cx.set_global(AvailableAudioDevices(devices)));
        cx.refresh();
    })
    .detach();
}

#[derive(Default)]
pub struct Audio {
    output: Option<(MixerDeviceSink, Mixer)>,
    active_sounds: Arc<AtomicUsize>,
    output_cleanup: Option<Task<()>>,
    pub echo_canceller: EchoCanceller,
    source_cache: HashMap<Sound, Buffered<Decoder<Cursor<Vec<u8>>>>>,
}

impl Global for Audio {}

impl Audio {
    fn ensure_output_exists(&mut self, output_audio_device: Option<DeviceId>) -> Result<&Mixer> {
        #[cfg(debug_assertions)]
        log::warn!(
            "Audio does not sound correct without optimizations. Use a release build to debug audio issues"
        );

        if self.output.is_none() {
            let (output_handle, output_mixer) =
                open_output_stream(output_audio_device, self.echo_canceller.clone())?;
            self.output = Some((output_handle, output_mixer));
        }

        Ok(self
            .output
            .as_ref()
            .map(|(_, mixer)| mixer)
            .expect("we only get here if opening the outputstream succeeded"))
    }

    pub fn play_sound(sound: Sound, cx: &mut App) {
        let output_audio_device = AudioSettings::get_global(cx).output_audio_device.clone();
        cx.update_default_global(|this: &mut Self, cx| {
            let source = this.sound_source(sound, cx).log_err()?;
            let output_mixer = this
                .ensure_output_exists(output_audio_device)
                .context("Could not get output mixer")
                .log_err()?
                .clone();

            this.play_source(source, &output_mixer, cx);
            Some(())
        });
    }

    fn play_source(
        &mut self,
        source: impl Source + Send + 'static,
        output_mixer: &Mixer,
        cx: &mut App,
    ) {
        let active_sounds = self.active_sounds.clone();
        active_sounds.fetch_add(1, Ordering::Relaxed);
        // Finite spans can end without polling `Done` for its final `None`.
        let source = source.constant_params(CHANNEL_COUNT, SAMPLE_RATE);
        output_mixer.add(Done::new(source, active_sounds.clone()));

        self.output_cleanup = Some(cx.spawn(async move |cx| {
            loop {
                let idle = active_sounds.load(Ordering::Relaxed) == 0;
                // The device may still have buffered samples after its sources are exhausted.
                cx.background_executor()
                    .timer(Duration::from_millis(100))
                    .await;
                if idle && active_sounds.load(Ordering::Relaxed) == 0 {
                    break;
                }
            }
            cx.update(Self::end_call);
        }));
    }

    pub fn end_call(cx: &mut App) {
        cx.update_default_global(|this: &mut Self, _cx| {
            this.output_cleanup.take();
            this.output.take();
            this.active_sounds = Arc::default();
        });
    }

    fn sound_source(&mut self, sound: Sound, cx: &App) -> Result<impl Source + use<>> {
        if let Some(wav) = self.source_cache.get(&sound) {
            return Ok(wav.clone());
        }

        let path = format!("sounds/{}.wav", sound.file());
        let bytes = cx
            .asset_source()
            .load(&path)?
            .map(anyhow::Ok)
            .with_context(|| format!("No asset available for path {path}"))??
            .into_owned();
        let cursor = Cursor::new(bytes);
        let source = Decoder::new(cursor)?.buffered();

        self.source_cache.insert(sound, source.clone());

        Ok(source)
    }
}

pub fn open_input_stream(
    device_id: Option<DeviceId>,
) -> anyhow::Result<rodio::microphone::Microphone> {
    let builder = rodio::microphone::MicrophoneBuilder::new();
    let builder = if let Some(id) = device_id {
        // TODO(jk): upstream patch
        // if let Some(input_device) = default_host().device_by_id(id) {
        //     builder.device(input_device);
        // }
        let mut found = None;
        for input in rodio::microphone::available_inputs()? {
            if input.clone().into_inner().id()? == id {
                found = Some(builder.device(input));
                break;
            }
        }
        found.unwrap_or_else(|| builder.default_device())?
    } else {
        builder.default_device()?
    };
    let stream = builder
        .default_config()?
        .prefer_sample_rates([
            SAMPLE_RATE,
            SAMPLE_RATE.saturating_mul(rodio::nz!(2)),
            SAMPLE_RATE.saturating_mul(rodio::nz!(3)),
            SAMPLE_RATE.saturating_mul(rodio::nz!(4)),
        ])
        .prefer_channel_counts([rodio::nz!(1), rodio::nz!(2), rodio::nz!(3), rodio::nz!(4)])
        .prefer_buffer_sizes(512..)
        .open_stream()?;
    log::info!("Opened microphone: {:?}", stream.config());
    Ok(stream)
}

pub fn resolve_device(device_id: Option<&DeviceId>, input: bool) -> anyhow::Result<cpal::Device> {
    if let Some(id) = device_id {
        if let Some(device) = default_host().device_by_id(id) {
            return Ok(device);
        }
        log::warn!("Selected audio device not found, falling back to default");
    }
    if input {
        default_host()
            .default_input_device()
            .context("no audio input device available")
    } else {
        default_host()
            .default_output_device()
            .context("no audio output device available")
    }
}

pub fn open_test_output(device_id: Option<DeviceId>) -> anyhow::Result<MixerDeviceSink> {
    let device = resolve_device(device_id.as_ref(), false)?;
    DeviceSinkBuilder::from_device(device)?
        .open_stream()
        .context("Could not open output stream")
}

pub fn open_output_stream(
    device_id: Option<DeviceId>,
    mut echo_canceller: EchoCanceller,
) -> anyhow::Result<(MixerDeviceSink, Mixer)> {
    let device = resolve_device(device_id.as_ref(), false)?;
    let mut output_handle = DeviceSinkBuilder::from_device(device)?
        .open_stream()
        .context("Could not open output stream")?;
    output_handle.log_on_drop(false);
    log::info!("Output stream: {:?}", output_handle);

    let (output_mixer, source) = rodio::mixer::mixer(CHANNEL_COUNT, SAMPLE_RATE);
    // otherwise the mixer ends as it's empty
    output_mixer.add(rodio::source::Zero::new(CHANNEL_COUNT, SAMPLE_RATE));
    let echo_cancelling_source = source // apply echo cancellation just before output
        .inspect_buffer::<BUFFER_SIZE, _>(move |buffer| {
            let mut buf: [i16; _] = buffer.map(|s| s.to_sample());
            echo_canceller.process_reverse_stream(&mut buf)
        });
    output_handle.mixer().add(echo_cancelling_source);

    Ok((output_handle, output_mixer))
}

#[derive(Clone, Debug)]
pub struct AudioDeviceInfo {
    pub id: DeviceId,
    pub desc: DeviceDescription,
}

impl AudioDeviceInfo {
    pub fn matches_input(&self, is_input: bool) -> bool {
        if is_input {
            self.desc.supports_input()
        } else {
            self.desc.supports_output()
        }
    }

    pub fn matches(&self, id: &DeviceId, is_input: bool) -> bool {
        &self.id == id && self.matches_input(is_input)
    }
}

impl std::fmt::Display for AudioDeviceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.desc.name(), self.id)
    }
}

fn get_available_audio_devices() -> Vec<AudioDeviceInfo> {
    let Some(devices) = default_host().devices().ok() else {
        return Vec::new();
    };
    devices
        .filter_map(|device| {
            let id = device.id().ok()?;
            let desc = device.description().ok()?;
            Some(AudioDeviceInfo { id, desc })
        })
        .collect()
}

#[derive(Default, Clone, Debug)]
pub struct AvailableAudioDevices(pub Vec<AudioDeviceInfo>);

impl Global for AvailableAudioDevices {}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;
    use std::time::{Duration, Instant};

    fn play_test_sound(output_mixer: &Mixer, cx: &mut TestAppContext) {
        cx.update(|cx| {
            cx.update_default_global(|audio: &mut Audio, cx| {
                audio.play_source(
                    rodio::static_buffer::StaticSamplesBuffer::new(
                        rodio::nz!(1),
                        rodio::nz!(1000),
                        &[0.25; 1000],
                    ),
                    output_mixer,
                    cx,
                );
            });
        });
    }

    #[gpui::test]
    fn test_cleanup_waits_for_overlapping_sounds(cx: &mut TestAppContext) {
        let (output_mixer, mut output) = rodio::mixer::mixer(rodio::nz!(1), rodio::nz!(1000));
        play_test_sound(&output_mixer, cx);
        cx.run_until_parked();
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.read_global::<Audio, _>(|audio, _| assert!(audio.output_cleanup.is_some()));

        assert_eq!(output.by_ref().take(500).count(), 500);
        play_test_sound(&output_mixer, cx);
        assert_eq!(output.by_ref().take(600).count(), 600);
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.read_global::<Audio, _>(|audio, _| {
            assert_eq!(audio.active_sounds.load(Ordering::Relaxed), 1);
            assert!(audio.output_cleanup.is_some());
        });

        output.count();
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.read_global::<Audio, _>(|audio, _| assert!(audio.output_cleanup.is_none()));
    }

    #[gpui::test]
    fn test_ending_call_does_not_stop_new_sound(cx: &mut TestAppContext) {
        let (old_mixer, old_output) = rodio::mixer::mixer(rodio::nz!(1), rodio::nz!(1000));
        play_test_sound(&old_mixer, cx);
        cx.run_until_parked();
        cx.update(Audio::end_call);

        let (new_mixer, new_output) = rodio::mixer::mixer(rodio::nz!(1), rodio::nz!(1000));
        play_test_sound(&new_mixer, cx);
        old_output.count();
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.read_global::<Audio, _>(|audio, _| {
            assert_eq!(audio.active_sounds.load(Ordering::Relaxed), 1);
            assert!(audio.output_cleanup.is_some());
        });

        new_output.count();
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.read_global::<Audio, _>(|audio, _| assert!(audio.output_cleanup.is_none()));
    }

    #[gpui::test]
    fn test_cleanup_after_buffered_notification(cx: &mut TestAppContext) {
        let (output_mixer, output) = rodio::mixer::mixer(CHANNEL_COUNT, SAMPLE_RATE);
        cx.update(|cx| {
            let source = Decoder::new(Cursor::new(
                include_bytes!("../../../assets/sounds/agent_done.wav").to_vec(),
            ))
            .expect("valid notification sound")
            .buffered();
            cx.update_default_global(|audio: &mut Audio, cx| {
                audio.play_source(source, &output_mixer, cx);
            });
        });
        output.count();
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.read_global::<Audio, _>(|audio, _| assert!(audio.output_cleanup.is_none()));
    }

    #[gpui::test]
    #[ignore = "requires an audio output device"]
    fn test_notification_releases_output(cx: &mut TestAppContext) {
        cx.update(|cx| {
            settings::init(cx);
            let source = Decoder::new(Cursor::new(
                include_bytes!("../../../assets/sounds/mute.wav").to_vec(),
            ))
            .expect("valid notification sound")
            .buffered();
            cx.default_global::<Audio>()
                .source_cache
                .insert(Sound::Mute, source);
            Audio::play_sound(Sound::Mute, cx);
            assert!(cx.global::<Audio>().output.is_some());
        });

        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(3) {
            // The device consumes audio outside GPUI's deterministic scheduler.
            std::thread::sleep(Duration::from_millis(10));
            cx.background_executor
                .advance_clock(Duration::from_millis(10));
            cx.run_until_parked();
        }

        cx.read_global::<Audio, _>(|audio, _| assert!(audio.output.is_none()));
    }
}
