# Analysis

Direct tracker fixtures independently joined raw bytes and calculated SHA-256, then independently counted Python code points and hashed reconstructed visible UTF-8. All 16 cases x 3 runs agreed with receipts.

No Unicode normalization is applied: composed `é` is one code point/two UTF-8 bytes and differs from decomposed `e + U+0301` (two code points/three bytes); hashes differ. SSE CRLF adds six raw bytes across three frames but does not change visible response hashing. NUL and controls survive JSON decode and visible hashing. A lone surrogate supplied as Python `str` is encoded with `errors=replace` as `?`.

Invalid `0xff` and `0xfe` content frames retain distinct raw SHA-256 values but both become U+FFFD and share response SHA-256 `83d544ccc223c057d2bf80d3f2a32982c32c3c0db8e2674820da5064783fb097`. This is deterministic lossy visible canonicalization; raw and visible hashes must be retained together. Split UTF-8 fragments are not reassembled by per-chunk decoding and end as `ERR_STREAM_EMPTY_VISIBLE_OUTPUT`.

Guard test: byte chunks are stringified by `str(raw_chunk)` at the guard boundary and become malformed-frame error output. This is outside its declared `Iterable[str]` contract, so it is a defect candidate, not a confirmed in-contract defect. No product code was changed; no smoke, network, provider, service, merge, or deploy action was performed.

Dissent: a consumer expecting byte-preserving guard input may treat the boundary behavior as a defect now. The counterargument is that accepting bytes is not the declared guard contract. Recommend a separate patch decision to either reject bytes explicitly or preserve bytes through the guard.
