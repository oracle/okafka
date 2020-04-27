This folder contains examples for demonstration of this client. To test these examples follow main README.md.

# package-consumer
This package contains examples for consuming messages from Transaction Event Queues.
## classes
`Consumer.java`: This is a sample example for consuming messages from transactional event queues. Provide necessary  properties required to connect to TEQ , consume messages from specified topic and then commit consumed messages.

# package-producer
This package contains examples for producing messages into Transaction Event Queues.
## classes
`Producer.java`: This is a sample example for producing messages into transactional event queues. Provide necessary properties required to connect to TEQ and produce messages into specified topic. If specified topic doesn't exist in TEQ then it creates a topic with single partition.