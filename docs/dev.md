to generate protobuf code 
```bash
protoc api/v1/*.proto --go_out=. --go_opt=paths=source_relative --proto_path=.
```

• Record — the data stored in our log.
• Store — the file we store records in.
• Index — the file we store index entries in.
• Segment — the abstraction that ties a store and an index together. 
• Log—the abstraction that ties all the segments together.
