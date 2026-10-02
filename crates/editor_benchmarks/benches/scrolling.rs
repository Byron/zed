use std::sync::Arc;

use assets::Assets;
use benchmarks::bench_utils::random_rust_file;
use buffer_diff::BufferDiff;
use editor::{Editor, EditorMode};
use gpui::{
    AppContext as _, AssetSource as _, BenchAppContext, Focusable as _, UpdateGlobal as _, point,
};
use language::{Buffer, Language, Rope};
use multi_buffer::MultiBuffer;
use rand::{SeedableRng as _, rngs::StdRng};
use settings::SettingsStore;
use theme::ActiveTheme as _;

const LINE_COUNT: usize = 600;

#[gpui::bench(
    inputs = [false, true],
    input_name = "minimap",
    group = "scroll_600_line_rust",
    sample_size = 10,
    fps = 120
)]
fn scrolling(minimap: &bool, cx: &mut BenchAppContext) {
    assert!(!cfg!(debug_assertions), "use --profile release-fast");

    cx.update(|cx| {
        settings::init(cx);
        theme_settings::init(theme::LoadThemes::JustBase, cx);
        editor::init(cx);
        SettingsStore::update_global(cx, |store, cx| {
            let show_minimap = if *minimap { "always" } else { "never" };
            store
                .set_user_settings(
                    &format!(
                        r#"{{
                            "buffer_font_size": 13,
                            "minimap": {{"show": "{show_minimap}", "display_in": "all_editors"}}
                        }}"#,
                    ),
                    cx,
                )
                .expect("valid benchmark settings");
        });

        let fonts = Assets
            .list("fonts")
            .expect("bundled font paths")
            .into_iter()
            .filter(|path| path.ends_with(".ttf"))
            .map(|path| {
                Assets
                    .load(&path)
                    .expect("read bundled font")
                    .expect("bundled font exists")
            })
            .collect();
        cx.text_system()
            .add_fonts(fonts)
            .expect("load bundled fonts");
    });

    let language = Arc::new(
        Language::new(
            grammars::load_config("rust"),
            Some(tree_sitter_rust::LANGUAGE.into()),
        )
        .with_queries(grammars::load_queries("rust"))
        .expect("load Rust queries"),
    );
    cx.update(|cx| language.set_theme(cx.theme().syntax()));
    let probe = "fn main() {}";
    assert!(
        !language
            .highlight_text(&Rope::from(probe), 0..probe.len())
            .is_empty()
    );

    let mut lines = random_rust_file(&mut StdRng::seed_from_u64(1), LINE_COUNT);
    let base_text = Arc::<str>::from(lines.join("\n"));
    for (index, line) in lines.iter_mut().enumerate() {
        if [22, 298, 562].contains(&index) {
            line.push_str(" // changed");
        }
    }
    let text = lines.join("\n");
    assert_eq!(text.lines().count(), LINE_COUNT);

    let (buffer, diff, diff_task) = cx.update(|cx| {
        let buffer = cx.new(|cx| Buffer::local(text.clone(), cx).with_language(language, cx));
        let snapshot = buffer.read(cx).text_snapshot();
        let diff = cx.new(|cx| BufferDiff::new(&snapshot, None, None, cx));
        let diff_task = diff.update(cx, |diff, cx| {
            diff.set_base_text(Some(base_text), snapshot, cx)
        });
        (buffer, diff, diff_task)
    });
    cx.run_until_idle();
    cx.update(|cx| {
        assert_eq!(
            *buffer.read(cx).parse_status().borrow(),
            language::ParseStatus::Idle
        );
        assert!(buffer.read(cx).snapshot().syntax_layers().next().is_some());
        let snapshot = buffer.read(cx).text_snapshot();
        assert_eq!(diff.read(cx).snapshot(cx).hunks(&snapshot).count(), 3);
    });
    drop(diff_task);

    let multi_buffer = cx.update(|cx| {
        cx.new(|cx| {
            let mut multi_buffer = MultiBuffer::singleton(buffer.clone(), cx);
            multi_buffer.add_diff(diff.clone(), cx);
            multi_buffer
        })
    });
    let mut window = cx.add_empty_window();
    let editor = window.update(|window, cx| {
        let editor = window.replace_root(cx, |window, cx| {
            Editor::new(EditorMode::full(), multi_buffer, None, window, cx)
        });
        window.focus(&editor.focus_handle(cx), cx);
        editor
    });
    cx.run_until_idle();
    let max_scroll_top = window.update(|_, cx| {
        let editor = editor.read(cx);
        assert_eq!(editor.minimap().is_some(), *minimap);
        let visible_lines = editor.visible_line_count().expect("editor was laid out");
        (LINE_COUNT as f64 - visible_lines - 1.0).min(300.0)
    });
    assert!(max_scroll_top > 3.0);

    let mut scroll_top = 0.0;
    let mut scroll_delta = 3.0;
    let mut scroll_steps = 0;
    cx.bench_renderer(editor.clone(), |editor, window, cx| {
        if scroll_top + scroll_delta > max_scroll_top || scroll_top + scroll_delta < 0.0 {
            scroll_delta = -scroll_delta;
        }
        scroll_top += scroll_delta;
        editor.set_scroll_position(point(0.0, scroll_top), window, cx);
        scroll_steps += 1;
    });

    assert!(scroll_steps > 0);
    window.update(|_, cx| {
        editor.update(cx, |editor, cx| {
            assert_eq!(editor.scroll_position(cx).y, scroll_top);
            assert_eq!(editor.text(cx), text);
            assert_eq!(editor.minimap().is_some(), *minimap);
        });
        let snapshot = buffer.read(cx).text_snapshot();
        assert_eq!(diff.read(cx).snapshot(cx).hunks(&snapshot).count(), 3);
    });
}

gpui::bench_group!(benches, scrolling);
gpui::bench_main!(benches);
