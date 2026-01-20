export default {
  tail(events: TraceItem[], env: Env, ctx: ExecutionContext) {
    // Metrics/analytics worker - skeleton implementation
    // TODO: Implement metrics collection and analytics logic
    console.log(events)
  },
} satisfies ExportedHandler<Env>
