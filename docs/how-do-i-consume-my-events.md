# How do I Consume My Events?

There are several great options here, and I'll share a few (Ruby-based) examples that we've run across in the course of building Changestream:

- The [Circuitry](https://github.com/kapost/circuitry) gem by Kapost makes it easy to create SQS queues and configure your SNS changestream topic to fan out to them.
- [Shoryuken](https://github.com/phstc/shoryuken) is a super efficient threaded AWS SQS message processor for Ruby that is influenced heavily by [Sidekiq](https://github.com/mperham/sidekiq).

