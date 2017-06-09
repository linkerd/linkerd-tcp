#!/bin/sh

set -e

version="1.0.2"
bin="target/namerd-${version}-exec"
sha="338428a49cbe5f395c01a62e06b23fa492a7a9f89a510ae227b46c915b07569e"
url="https://github.com/linkerd/linkerd/releases/download/${version}/namerd-${version}-exec"

validbin() {
  checksum=$(openssl dgst -sha256 $bin | awk '{ print $2 }')
  [ "$checksum" = $sha ]
}

if [ -f "$bin" ] && ! validbin ; then
    echo "bad $bin" >&2
    mv "$bin" "${bin}.bad"
fi

if [ ! -f "$bin" ]; then
  echo "downloading $bin" >&2
  curl -L --silent --fail -o "$bin" "$url"
  chmod 755 "$bin"
fi

if ! validbin ; then
    echo "bad $bin. delete $bin and run $0 again." >&2
    exit 1
fi

mkdir -p ./tmp.discovery
if [ ! -f ./tmp.discovery/default ]; then
    echo "127.1 9991" > ./tmp.discovery/default
fi

"$bin" -- - <<EOF
admin:
  port: 9991

namers:
  - kind: io.l5d.fs
    rootDir: ./tmp.discovery

storage:
  kind: io.l5d.inMemory
  namespaces:
    default: /svc => /#/io.l5d.fs;

interfaces:
  - kind: io.l5d.httpController
EOF
