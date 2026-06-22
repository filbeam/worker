/**
 * Redirects legacy `*.filcdn.io` requests to the equivalent `*.filbeam.io` URL
 * with a 301.
 *
 * @param {Request} request
 * @returns {Response | undefined} A redirect response, or `undefined` when the
 *   request is not for a legacy domain.
 */
export function redirectLegacyDomain(request) {
  if (URL.parse(request.url)?.hostname.endsWith('filcdn.io')) {
    return Response.redirect(
      request.url.replace('filcdn.io', 'filbeam.io'),
      301,
    )
  }
}
