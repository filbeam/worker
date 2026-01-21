export default {
  async tail(events: TraceItem[], env: Env, ctx: ExecutionContext) {
    for (const event of events) {
      const serviceName = extractServiceName(event.scriptTags, event.scriptName)
      const responseStatus = extractResponseStatus(event)

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

function extractResponseStatus(event: TraceItem): number {
  const fetchEvent = event.event as TraceItemFetchEventInfo | null
  return fetchEvent?.response?.status ?? 0
}
