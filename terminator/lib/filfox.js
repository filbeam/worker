import assert from 'assert'

export const getRecentSendMessage = async () => {
  let res = await fetch('https://filfox.info/api/v1/message/list?method=Send')
  if (!res.ok) {
    const err = new Error(
      `Filfox request failed with ${res.status}: ${(await res.text()).trimEnd()}`,
    )
    err.code = 'FILFOX_REQUEST_FAILED'
    throw err
  }
  const body = await res.json()
  assert(body.messages.length > 0, '/message/list returned an empty list')
  const sendMsg = body.messages.find((m) => m.method === 'Send')
  assert(sendMsg, 'No Send message found in the recent committed messages')
  const cid = sendMsg.cid

  res = await fetch(`https://filfox.info/api/v1/message/${cid}`)
  if (!res.ok) {
    throw new Error(
      `Filfox request failed with ${res.status}: ${(await res.text()).trimEnd()}`,
    )
  }

  return await res.json()
}
