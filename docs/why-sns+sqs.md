# Why SNS+SQS?

For anyone who is unfamilar with the technology, [Fabrizio Branca](https://github.com/fbrnc) has some great reading on [the differences between SNS, SQS, and Kinesis](http://fbrnc.net/blog/2016/03/messaging-on-aws).

SNS+SQS is a reliable, simple, and massively scalable distributed message bus for large applications due to its [straightforward API](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html), [automatic visibility timeout](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/MessageLifecycle.html), [rapid autoscaling](http://docs.aws.amazon.com/autoscaling/latest/userguide/as-using-sqs-queue.html), and [Fan-out](http://docs.aws.amazon.com/sns/latest/dg/SNS_Scenarios.html) capabilities.

SQS and/or SNS is trusted by many companies:

- [Dropbox](https://www.youtube.com/watch?v=mP46FviScYQ)
- [Unbounce](http://inside.unbounce.com/product-dev/aws-messaging-patterns/)
- [Kapost](http://engineering.kapost.com/2015/07/decoupling-ruby-applications-with-amazon-sns-sqs/)
