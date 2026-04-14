fn main() {
    #[cfg(feature = "remote-grpc")]
    {
        let proto_file = "proto/scheduler.proto";

        // Use protox (pure Rust) as the protobuf compiler so we don't need
        // a system `protoc` binary.
        let file_descriptors = protox::compile([proto_file], ["proto/"]).unwrap();

        let file_descriptor_path =
            std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap())
                .join("scheduler_descriptor.bin");

        // protox returns a prost_types::FileDescriptorSet which implements prost::Message.
        // Use prost::Message::encode_to_vec via the re-export.
        let bytes = protox::prost::Message::encode_to_vec(&file_descriptors);
        std::fs::write(&file_descriptor_path, bytes).unwrap();

        tonic_build::configure()
            .skip_protoc_run()
            .file_descriptor_set_path(&file_descriptor_path)
            .compile_protos(&[proto_file], &["proto/"])
            .expect("Failed to compile proto");
    }
}
