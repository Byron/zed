set positional-arguments

default:
    @just --list

[macos]
zed-bundle-macos *args:
    MACOS_SIGNING_KEY="${MACOS_SIGNING_KEY:-Rustty Local Signing}" script/bundle-mac-release-fast-small "$@"
