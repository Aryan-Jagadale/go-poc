| Operation | Unbuffered | Buffered | Closed | Nil |
|---|---|---|---|---|
| **send** | blocks until a receiver | blocks only when full | **panics** | blocks forever |
| **receive** | blocks until a sender | blocks only when empty | returns `zero, false` at once | blocks forever |
| **close** | ok | ok | **panics** | **panics** |