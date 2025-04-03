import * as esbuild from "https://deno.land/x/esbuild@v0.19.2/mod.js";
import { walk } from "https://deno.land/std@0.224.0/fs/walk.ts";
import { minify as minifyHtml } from "npm:html-minifier-terser@7.2.0";

const SITE_DIR = "_site";

// Get file size in bytes
async function getFileSize(filePath: string): Promise<number> {
  try {
    const stat = await Deno.stat(filePath);
    return stat.size;
  } catch {
    return 0;
  }
}

// Process a single file
async function processFile(filePath: string): Promise<void> {
  const ext = filePath.slice(filePath.lastIndexOf("."));
  
  try {
    const originalSize = await getFileSize(filePath);
    const content = await Deno.readTextFile(filePath);
    let minified: string;
    
    if (ext === ".html") {
      minified = await minifyHtml(content, {
        collapseWhitespace: true,
        minifyCSS: true,
        minifyJS: true,
        removeComments: true,
      });
    } else {
      const result = await esbuild.transform(content, {
        minify: true,
        loader: ext.slice(1) as "js" | "css",
      });
      minified = result.code;
    }
    
    await Deno.writeTextFile(filePath, minified);
    const newSize = await getFileSize(filePath);
    const reduction = ((originalSize - newSize) / originalSize) * 100;
    
    console.log(
      `✅ Minified ${ext.slice(1).toUpperCase()}: ${filePath}
      | ${originalSize}B → ${newSize}B (${reduction.toFixed(2)}% reduction) |`
    );
  } catch (err) {
    console.error(`❌ Failed to minify ${filePath}:`, err);
  }
}

async function main() {
  try {
    // Calculate overall folder size
    let originalTotalSize = 0;
    const filesToProcess: string[] = [];
    
    // Find all files to process and calculate initial total size in one pass
    for await (const entry of walk(SITE_DIR, { includeDirs: false })) {
      const ext = entry.path.slice(entry.path.lastIndexOf("."));
      if ([".css", ".js", ".html"].includes(ext)) {
        filesToProcess.push(entry.path);
      }
      
      try {
        const stat = await Deno.stat(entry.path);
        originalTotalSize += stat.size;
      } catch {
        // Skip files we can't stat
      }
    }
    
    // Process files in parallel
    const CONCURRENCY_LIMIT = 5;
    for (let i = 0; i < filesToProcess.length; i += CONCURRENCY_LIMIT) {
      const batch = filesToProcess.slice(i, i + CONCURRENCY_LIMIT);
      await Promise.all(batch.map(file => processFile(file)));
    }
    
    // Calculate final size
    let newTotalSize = 0;
    for await (const entry of walk(SITE_DIR, { includeDirs: false })) {
      try {
        const stat = await Deno.stat(entry.path);
        newTotalSize += stat.size;
      } catch {
        // Skip files we can't stat
      }
    }
    
    const totalReduction = ((originalTotalSize - newTotalSize) / originalTotalSize) * 100;
    
    console.log("🎉 Optimization complete!");
    console.log(
      `📂 Directory size: ${originalTotalSize}B → ${newTotalSize}B (${totalReduction.toFixed(2)}% reduction)`
    );
  } finally {
    esbuild.stop();
  }
}

await main();