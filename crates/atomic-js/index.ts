// Handwritten TypeScript loader for @atomic-compute/js.
//
// Replaces the napi-rs auto-generated index.js with a typed, curated loader
// that only supports the platforms this project officially targets:
//   - macOS arm64
//   - Linux x86_64 (glibc and musl)
//   - Linux arm64  (glibc and musl)
//
// Compiled to dist/index.js by tsc. All .node paths are resolved relative to
// the package root (one level above dist/) via PKG_ROOT.

/* eslint-disable @typescript-eslint/no-require-imports */
import { join } from 'node:path'
import { readFileSync } from 'node:fs'

// Package root is one level above this compiled file (dist/ → package root).
const PKG_ROOT = join(__dirname, '..')

const PACKAGE_VERSION = '0.1.0'

type NativeBinding = Record<string, unknown>

function enforceVersion(pkg: string): void {
  const meta = require(`${pkg}/package.json`) as { version: string }
  const check = process.env['NAPI_RS_ENFORCE_VERSION_CHECK']
  if (check && check !== '0' && meta.version !== PACKAGE_VERSION) {
    throw new Error(
      `@atomic-compute/js: native binding version mismatch for ${pkg}: ` +
      `expected ${PACKAGE_VERSION}, got ${meta.version}.`
    )
  }
}

function tryLoad(localFile: string, npmPkg: string): NativeBinding | null {
  // Prefer the locally bundled .node file (avoids network dependency).
  try {
    return require(localFile) as NativeBinding
  } catch {
    // Not present; try the optional npm package.
  }
  try {
    const b = require(npmPkg) as NativeBinding
    enforceVersion(npmPkg)
    return b
  } catch {
    return null
  }
}

function isMusl(): boolean {
  // Primary check: read ldd — reliably present on all glibc and musl systems.
  try {
    return readFileSync('/usr/bin/ldd', 'utf-8').includes('musl')
  } catch {
    // /usr/bin/ldd absent — fall through to diagnostics report.
  }

  type DiagnosticsInfo = {
    header?: { glibcVersionRuntime?: string }
    sharedObjects?: string[]
  }
  type ExtProcess = NodeJS.Process & {
    report?: {
      excludeNetwork?: boolean
      getReport?(): DiagnosticsInfo
    }
  }
  const report = (process as ExtProcess).report
  if (typeof report?.getReport === 'function') {
    report.excludeNetwork = true
    const info: DiagnosticsInfo = report.getReport()
    if (info.header?.glibcVersionRuntime) return false
    return (info.sharedObjects ?? []).some(
      (lib: string) => lib.includes('libc.musl-') || lib.includes('ld-musl-')
    )
  }
  return false
}

function loadBinding(): NativeBinding {
  // Allow overriding the .node path for testing / custom deployments.
  if (process.env['NAPI_RS_NATIVE_LIBRARY_PATH']) {
    return require(process.env['NAPI_RS_NATIVE_LIBRARY_PATH']) as NativeBinding
  }

  const { platform, arch } = process

  if (platform === 'darwin') {
    if (arch !== 'arm64') {
      throw new Error(
        `@atomic-compute/js: macOS/${arch} is not supported (supported: arm64).`
      )
    }
    const b = tryLoad(
      join(PKG_ROOT, 'index.darwin-arm64.node'),
      '@atomic-compute/js-darwin-arm64'
    )
    if (b) return b
  } else if (platform === 'linux') {
    if (arch !== 'x64' && arch !== 'arm64') {
      throw new Error(
        `@atomic-compute/js: Linux/${arch} is not supported (supported: x64, arm64).`
      )
    }
    const libc = isMusl() ? 'musl' : 'gnu'
    const b = tryLoad(
      join(PKG_ROOT, `index.linux-${arch}-${libc}.node`),
      `@atomic-compute/js-linux-${arch}-${libc}`
    )
    if (b) return b
  } else {
    throw new Error(
      `@atomic-compute/js: ${platform} is not supported. ` +
      `Supported platforms: darwin (arm64), linux (x64, arm64).`
    )
  }

  throw new Error(
    `@atomic-compute/js: failed to load native binding for ${platform}/${arch}. ` +
    `Try rebuilding with: cd crates/atomic-js && npm run build`
  )
}

export = loadBinding()
