import {
  UsageReported as DataSetUsageReportedEvent,
} from '../generated/FilecoinBeam/FilecoinBeam'
import { UsageReported } from '../generated/schema'
import { getEventEntityId } from './utils'

export function handleDataSetUsageReported(
  event: DataSetUsageReportedEvent,
): void {
  const entity = new UsageReported(getEventEntityId(event))
  entity.dataSetId = event.params.dataSetId.toString()
  entity.epoch = event.params.epoch.toString()
  entity.cdnBytesUsed = event.params.cdnBytesUsed.toString()
  entity.cacheMissBytesUsed = event.params.cacheMissBytesUsed.toString()

  entity.save()
}
