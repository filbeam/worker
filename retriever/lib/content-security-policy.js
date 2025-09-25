// List of allowed hosts in the CSP <host-source> format:
// https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Security-Policy#host-source
const ALLOWED_HOSTS = [
  'https://*.filbeam.io',

  // Other service serving content-addressable or static assets
  'https://*.w3s.link',
  'https://*.dweb.link',
  'https://*.githubusercontent.com',
]

/**
 * @param {Response} response A Response object we can modify (i.e. you must
 *   clone the Reponse object returned by `fetch` before passing it to this
 *   function).
 */
export function setContentSecurityPolicy(response) {
  // This functions sets the Content Security Policy (CSP) header for the response.
  // CSP is a security feature that helps prevent attacks like Cross-Site Scripting (XSS) by specifying which sources of content are allowed to be loaded by the browser.
  // Learn more:
  //   https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP
  //   https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CSP
  //
  // Our implementation is inspired by w3s.link:
  // https://github.com/storacha/w3link/blob/d73e3783c4c520e85e96dba1a2eb507da0f3cbb3/packages/edge-gateway-link/src/gateway.js#L74-L98

  const allowedHostsAsString = ALLOWED_HOSTS.join(' ')

  // The `default-src` directive controls the default sources for most content types.
  // - `'self'` allows content from the same origin.
  // - `'unsafe-inline'` and `'unsafe-eval'` allow inline scripts and eval (not recommended for strong security, but sometimes needed for legacy code).
  // - `blob:` and `data:` allow loading resources from blob and data URLs.
  // - `${allowedHostsAsString}` allows content from the specified external hosts.
  // Docs: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy/default-src
  const defaultSrc = `'self' 'unsafe-inline' 'unsafe-eval' blob: data: ${allowedHostsAsString}`

  // Set the CSP header with various directives:
  // - `default-src`: as described above.
  // - `form-action 'self'`: restricts where forms can be submitted. Docs: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy/form-action
  // - `navigate-to 'self'`: restricts which URLs the document can navigate to. Docs: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy/navigate-to
  response.headers.set(
    'content-security-policy',
    `default-src ${defaultSrc}; form-action 'self'; navigate-to 'self';`,
  )
}
