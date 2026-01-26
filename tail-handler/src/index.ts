export default {
  async tail(events: TraceItem[], env: Env, ctx: ExecutionContext) {
    for (const event of events) {
      const serviceName = extractServiceName(event.scriptTags, event.scriptName)
      // CF typings for `writeDataPoint` don't allow `undefined` values, despite the fact that
      // the runtime does accept them and treats them as SQL value `null`.
      const responseStatus = extractResponseStatus(event) as unknown as number

      try {
        env.RETRIEVAL_STATS.writeDataPoint({
          indexes: [serviceName],
          doubles: [event.wallTime, event.cpuTime, responseStatus],
          blobs: [event.outcome],
        })
      } catch (error) {
        console.error('Failed to write data point for event', error)
      }
    }
  },
} satisfies ExportedHandler<Env>

function extractServiceName(
  scriptTags: string[] | undefined,
  scriptName: string | null,
): string {
  const prefix = 'cf:service='
  const tag = scriptTags?.find((t) => t.startsWith(prefix))
  return tag ? tag.slice(prefix.length) : (scriptName ?? 'unknown')
}

function extractResponseStatus(event: TraceItem): number | undefined {
  const fetchEvent = event.event as TraceItemFetchEventInfo | null
  return fetchEvent?.response?.status
}
