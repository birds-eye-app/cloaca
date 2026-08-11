#!/usr/bin/env python3
"""Bundle AVITYPE into one self-contained HTML file.

Inlines manifest.json and every audio clip as base64 so the page makes no
network requests (needed for CSP-restricted hosts like Claude artifacts).
Also strips the <!doctype>/<html>/<head>/<body> wrapper when --bare is given
(artifact pages supply their own document skeleton).

Usage: python tools/build_artifact.py [--bare] [out.html]
"""
import base64
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
GAME = os.path.dirname(HERE)


def main() -> None:
    bare = "--bare" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    out_path = args[0] if args else os.path.join(GAME, "avitype-bundle.html")

    with open(os.path.join(GAME, "index.html")) as f:
        src = f.read()

    with open(os.path.join(GAME, "manifest.json")) as f:
        manifest = json.load(f)
    audio = {}
    for m in manifest:
        p = os.path.join(GAME, m["file"])
        if os.path.exists(p):
            with open(p, "rb") as af:
                audio[m["slug"]] = base64.b64encode(af.read()).decode()

    inject = ("<script>window.AVITYPE_MANIFEST=" + json.dumps(manifest) +
              ";window.AVITYPE_AUDIO=" + json.dumps(audio) + ";</script>\n")
    src = src.replace("<script>\n(()=>{", inject + "<script>\n(()=>{", 1)

    if bare:
        head = src.split("<head>")[1].split("</head>")[0]
        head = "\n".join(l for l in head.splitlines()
                         if not l.strip().startswith("<meta"))
        body = src.split("<body>")[1].rsplit("</body>")[0]
        src = head.strip() + "\n" + body.strip() + "\n"

    with open(out_path, "w") as f:
        f.write(src)
    print(f"wrote {out_path} ({os.path.getsize(out_path)//1024}KB, "
          f"{len(audio)} clips inlined)")


if __name__ == "__main__":
    main()
