# io2

io library

This library gives a single threaded async executor that uses io_uring under the hood.
It supports polled (not interrupt driven) direct_io for disk io for minimum latency and maximum throughput.

Main priorities are simplicity, stability and efficiency.

## Requirements for Direct-IO usage

- Need to have linux kernel version >= 6.1
- Need to use ext4 or xfs.
- Need to enable io polling on your NVMe disk by setting nvme.poll_queues kernel parameter to at least 1

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
