// Package web embeds the browser console so the server binary is self-contained.
package web

import "embed"

//go:embed index.html app.js style.css
var Assets embed.FS
