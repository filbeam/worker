#!/usr/bin/env node

/*
This is a script to update TypeScript project references to match
the npm workspace structure.

Inspired by https://github.com/loopbackio/loopback-next/blob/master/bin/update-ts-project-refs.js
*/

import { spawn } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import * as path from 'node:path'

// Ignore the fact that @npmcli/map-workspaces does not have TypeScript types
// @ts-ignore
import mapWorkspaces from '@npmcli/map-workspaces'
import { once } from 'node:events'

const debug = process.env.DEBUG ? console.log : () => {}

const rootDir = path.resolve(import.meta.dirname, '..')
debug('Project root', rootDir)

const rootPackageJson = await readJsonFile(path.join(rootDir, 'package.json'))

const rootTsConfigPath = path.join(rootDir, 'tsconfig.json')
debug('Root tsconfig path: %s', rootTsConfigPath)

const commonTsConfigPath = path.join(rootDir, 'tsconfig.base.json')
debug('Common tsconfig path: %s', commonTsConfigPath)

const workspaces = await mapWorkspaces({ pkg: rootPackageJson, cwd: rootDir })
debug('Workspaces: %o', workspaces)

/** @type {string[]} */
const rootRefs = []

for (const [name, location] of workspaces) {
  const tsconfigPath = path.join(location, 'tsconfig.json')
  rootRefs.push(tsconfigPath)

  const packageJsonPath = path.join(location, 'package.json')
  /**
   * @type {{
   *   dependencies: Record<string, string>
   *   devDependencies: Record<string, string>
   *   optionalDependencies: Record<string, string>
   *   peerDependencies: Record<string, string>
   * }}
   */
  const packageMeta = /** @type {any} */ (await readJsonFile(packageJsonPath))

  const defaultTsConfig = {
    $schema: 'https://json.schemastore.org/tsconfig',
    extends: path.relative(path.dirname(tsconfigPath), commonTsConfigPath),
    compilerOptions: {
      composite: true,
      outDir: 'dist',
    },
    include: ['**/*.ts', '**/*.js', 'src/**/*.json'],
    exclude: ['dist', 'test'],
  }

  const localDeps = [...workspaces.keys()].filter(
    (pkgName) =>
      pkgName in (packageMeta.dependencies || {}) ||
      pkgName in (packageMeta.devDependencies || {}) ||
      pkgName in (packageMeta.optionalDependencies || {}) ||
      pkgName in (packageMeta.peerDependencies || {}),
  )

  // Sort the references so that we will have a consistent output
  localDeps.sort()
  debug('Local deps for %s: %o', name, localDeps)

  const originalTsConfig = /** @type {Record<string, unknown>} */ (
    await readJsonFile(tsconfigPath, null)
  )
  const tsconfig = structuredClone(originalTsConfig ?? defaultTsConfig)
  tsconfig.references = localDeps.map((pkgName) => {
    const link = path.relative(
      path.dirname(tsconfigPath),
      path.join(workspaces.get(pkgName), 'tsconfig.json'),
    )
    return { path: link }
  })

  // It's important to preserve the modification time of tsconfig.json files
  // to avoid unnecessary rebuilds by TypeScript compiler.
  if (JSON.stringify(tsconfig) === JSON.stringify(originalTsConfig)) {
    console.log(' - %s', tsconfigPath)
  } else {
    console.log(' ✓ %s', tsconfigPath)
    await writeJsonFile(tsconfigPath, tsconfig)
  }
}

const originalRootConfig = /** @type {Record<string, unknown>} */ (
  await readJsonFile(rootTsConfigPath)
)
const rootTsConfig = structuredClone(originalRootConfig)
rootTsConfig.references = rootRefs.map((refPath) => ({
  path: path.relative(rootDir, refPath),
}))
if (JSON.stringify(rootTsConfig) === JSON.stringify(originalRootConfig)) {
  console.log(' - %s', rootTsConfigPath)
} else {
  console.log('Writing updated root tsconfig to %s', rootTsConfigPath)
  await writeJsonFile(rootTsConfigPath, rootTsConfig)
  console.log(' ✓ %s', rootTsConfigPath)
}

const child = spawn('npm', ['run', 'lint:fix'], {
  cwd: rootDir,
  stdio: 'inherit',
})
await once(child, 'close')
if (child.exitCode !== 0) {
  console.error(
    'npm run lint:fix failed with exit code %s signal code %s',
    child.exitCode,
    child.signalCode,
  )
  process.exit(1)
}

/**
 * @param {string} filePath
 * @param {unknown | undefined} defaultValue
 * @returns {Promise<unknown>}
 */
async function readJsonFile(filePath, defaultValue = undefined) {
  try {
    const text = await readFile(filePath, 'utf-8')
    try {
      return JSON.parse(text)
    } catch (err) {
      if (typeof err === 'object' && err) {
        Object.assign(err, { filePath })
      }
      throw err
    }
  } catch (err) {
    if (
      typeof err === 'object' &&
      err &&
      'code' in err &&
      err.code === 'ENOENT' &&
      defaultValue !== undefined
    ) {
      return defaultValue
    }
    throw err
  }
}

/**
 * @param {string} filePath
 * @param {unknown} jsonObject
 */
async function writeJsonFile(filePath, jsonObject) {
  const text = JSON.stringify(jsonObject, null, 2) + '\n'
  await writeFile(filePath, text, 'utf-8')
}
