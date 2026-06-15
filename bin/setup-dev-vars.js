#!/usr/bin/env node

/*
This script creates .dev.vars files from .dev.vars.template files.
It's useful for setting up local development environments and for CI.

- If .dev.vars doesn't exist: copies the template
- If .dev.vars already exists: skips (won't overwrite existing files)
*/

import { copyFile, access, glob } from 'node:fs/promises'
import * as path from 'node:path'

const rootDir = path.resolve(import.meta.dirname, '..')

const templateFiles = await Array.fromAsync(
  glob('*/.dev.vars.template', { cwd: rootDir }),
)

for (const templateRelPath of templateFiles) {
  const templatePath = path.join(rootDir, templateRelPath)
  const targetPath = templatePath.replace(/\.template$/, '')
  const targetRelPath = path.relative(rootDir, targetPath)

  const exists = await fileExists(targetPath)
  if (exists) {
    console.log(' - %s (already exists)', targetRelPath)
  } else {
    await copyFile(templatePath, targetPath)
    console.log(' ✓ %s', targetRelPath)
  }
}

/**
 * @param {string} filePath
 * @returns {Promise<boolean>}
 */
async function fileExists(filePath) {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}
