#[cfg(feature = "remote-http")]
pub mod http;

#[cfg(feature = "remote-http")]
pub use self::http::{HttpDispatcher, HttpResultReceiver, WorkerHttpServer};

#[cfg(feature = "remote-grpc")]
pub mod grpc;

#[cfg(feature = "remote-grpc")]
pub use self::grpc::{GrpcDispatcher, GrpcResultReceiver, WorkerGrpcServer};

#[cfg(feature = "remote-rabbitmq")]
pub mod rabbitmq;

#[cfg(feature = "remote-rabbitmq")]
pub use self::rabbitmq::{RabbitMqDispatcher, RabbitMqResultReceiver, RabbitMqWorkerRunner};
