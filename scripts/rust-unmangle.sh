#!/bin/sed -rf
# Unmangle Rust symbols
# Borrowed from https://github.com/Yamakaky/rust-unmangle
# See https://git.kernel.org/cgit/linux/kernel/git/tip/tip.git/commit/?id=cae15db74999edb96dd9f5bbd4d55849391dd92b
# Example, with [FlameGraph](https://github.com/brendangregg/FlameGraph):
#     perf record -g target/debug/bin
#     perf script | stackcollapse-perf | rust-unmangle | flamegraph > perf.svg

# Remove hash and address offset
s/::h[0-9a-f]{16}//g
s/\+0x[0-9a-f]+//g

# Convert special characters
s/\$C\$/,/g
s/\$SP\$/@/g
s/\$BP\$/*/g
s/\$RF\$/\&/g
s/\$LT\$/</g
s/\$GT\$/>/g
s/\$LP\$/(/g
s/\$RP\$/)/g
s/\$u20\$/ /g
s/\$u27\$/'/g
s/\$u5b\$/[/g
s/\$u5d\$/]/g
s/\$u7b\$/{/g
s/\$u7d\$/}/g
s/\$u7e\$/~/g

# Fix . and _
s/\.\./::/g
s/[^\.]\.[^\.]/./g
s/([;:])_/\1/g
