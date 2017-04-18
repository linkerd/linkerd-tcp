Profiling linkerd-tcp
=====================

Building FlameGraphs for linkerd-tcp under Linux
------------------------------------------------
Ensure that `perf` is installed
```
sudo apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r`
```

Clone a copy of Brendan Gregg's awesome `FlameGraph` repository
```
git clone --depth=1 https://github.com/brendangregg/FlameGraph
```

Run `linkerd-tcp` under `linux perf` [samplerate](#1-setting-a-useful-sample-rate)
```
RUST_BACKTRACE=full perf record -g -F 999 target/release/linkerd-tcp example.yml
```

Send it traffic through your favorite load tester.
```
slow_cooker_linux_amd64 -H 'default' -totalRequests 1000000 -qps 10 -concurrency 100 http://proxy-test-4e:7474/
```

Once your load test is complete, use the linkerd-tcp admin port to shutdown
the process.
```
curl -X POST http://localhost:9989/shutdown
```

Build your flamegraph
```
perf script | stackcollapse-perf.pl | rust-unmangle.sh | flamegraph.pl > linkerd-tcp-flame.svg
```

Copy `linkerd-tcp-flame.svg` to your local machine and dig in with a
SVG viewer like [Gapplin](https://itunes.apple.com/us/app/gapplin/id768053424?mt=12)
for the Mac.

Footnotes
---------

#### [1] Setting a Useful Sample Rate
When setting a sample rate (`-F`), remember that the Nyquist theorem tells us
that to see events which happen at a rate of N you'll need to sample with a
rate of 2N.