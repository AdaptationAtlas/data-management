// @ts-nocheck
import * as esbuild from "https://deno.land/x/esbuild@v0.25.5/mod.js";
import { walk } from "https://deno.land/std@0.224.0/fs/walk.ts";
import { extname } from "https://deno.land/std@0.224.0/path/extname.ts";
import { minify as minifyHtml } from "npm:html-minifier-terser@7.2.0";

const SITE_DIR = "_book";
const CONCURRENCY = 5;
const loaders = { ".js": "js", ".css": "css" } as const;

const getSize = async (path: string) =>
	(await Deno.stat(path).catch(() => ({ size: 0 }))).size;
const formatSize = (bytes: number) =>
	bytes > 1e6 ? `${(bytes / 1e6).toFixed(2)}MB` : `${bytes}B`;

async function processFile(
	path: string,
	originalSize: number,
): Promise<number> {
	const ext = extname(path);
	try {
		const content = await Deno.readTextFile(path);
		let minified: string;

		if (ext === ".html") {
			minified = await minifyHtml(content, {
				collapseWhitespace: true,
				minifyCSS: true,
				minifyJS: true,
				removeComments: true,
				removeRedundantAttributes: true,
				removeEmptyAttributes: true,
				useShortDoctype: true,
			});
		} else if (loaders[ext]) {
			minified = (
				await esbuild.transform(content, {
					minify: true,
					loader: loaders[ext],
				})
			).code;
		} else return originalSize;

		await Deno.writeTextFile(path, minified);
		const newSize = await getSize(path);
		const reduction = ((originalSize - newSize) / originalSize) * 100;
		console.log(
			`${path}: ${formatSize(originalSize)} → ${formatSize(newSize)} (${reduction.toFixed(1)}%)`,
		);
		return newSize;
	} catch (err) {
		console.error(`Failed: ${path}`, err.message);
		return originalSize;
	}
}

async function main() {
	try {
		const files: { path: string; size: number }[] = [];

		// Collect files
		for await (const entry of walk(SITE_DIR, { includeDirs: false })) {
			const ext = extname(entry.path);
			if ([".css", ".js", ".html"].includes(ext)) {
				const size = await getSize(entry.path);
				if (size > 0) files.push({ path: entry.path, size });
			}
		}

		let originalTotal = 0,
			newTotal = 0;

		// Process in batches
		for (let i = 0; i < files.length; i += CONCURRENCY) {
			const batch = files.slice(i, i + CONCURRENCY);
			const results = await Promise.all(
				batch.map((f) => {
					originalTotal += f.size;
					return processFile(f.path, f.size);
				}),
			);
			newTotal += results.reduce((sum, size) => sum + size, 0);
		}

		const reduction = ((originalTotal - newTotal) / originalTotal) * 100;
		console.log(
			`\nOptimized: ${formatSize(originalTotal)} → ${formatSize(newTotal)} (${reduction.toFixed(1)}%)`,
		);
	} finally {
		esbuild.stop();
	}
}

await main();
