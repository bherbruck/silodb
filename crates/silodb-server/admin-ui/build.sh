#!/bin/sh
# Rebuild the admin UI and refresh the committed dist the server embeds.
# Needs: cargo install dioxus-cli && rustup target add wasm32-unknown-unknown
set -e
cd "$(dirname "$0")"
dx build --release
rm -rf ../ui-dist
cp -r target/dx/silodb-admin-ui/release/web/public ../ui-dist
echo "ui-dist refreshed — commit it"
