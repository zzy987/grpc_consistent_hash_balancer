# GRPC Consistent Hash Balancer

A demo of a grpc balancer using consistent hash

### Features

- Consistent Hash Ring: All servers are abstracted into a hash ring in the picker, implemented in /client/balancer/consistent_hash_picker.

- On-demand Connections Setting up: Different from the default behavior of grpc to set up connections with all servers, I use a trick to make connections set up on-demand, implemented in /client/balancer/consistent_hash_balancer.

- Retry in Interceptor: Unlike the retry example of grpc which implements retry ability with a service config, I implement it in the interceptor, for service config based retry Because retry based on service config will ignore some errors in streaming. (This has not been decided yet.)

- Connection Survival Management: The balancer will reset a connection to idle when it has not been using for too long, for it seems like grpc will not close any connection unless some network exception happens. (This has not finished yet.)
