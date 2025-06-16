validate_s3_class <- function(s3) {
  # not a named class, so tough to validate...
  if (!is.list(s3)) {
    return(FALSE)
  }

  required_methods <- c(
    # random selection of methods to check against
    "put_bucket_policy",
    "list_objects_v2",
    ".internal",
    "get_object"
  )
  all(required_methods %in% names(s3))
}

#' S3 Directory Uploader Class
#'
#' An R6 class for uploading directories and files to Amazon S3 with support
#' for pattern filtering, custom naming functions, and parallel uploads.
#'
#' @description
#' This class provides a comprehensive interface for uploading local directories
#' to Amazon S3 buckets. It supports file filtering by patterns, custom naming
#' transformations, validation of S3 connections, and both sequential and
#' parallel upload modes.
#' @export
S3DirUploader <- R6::R6Class(
  "S3_uploader",
  public = list(
    #' @field upload_id character
    upload_id = NULL,
    #' @field bucket character. Name of the S3 bucket for uploads.
    bucket = NULL,
    #' @field public logical. Whether to make uploaded folder public read/list.
    public = NULL,
    #' @field recursive logical. Whether to include sub-directories in upload.
    recursive = FALSE,
    #' @field s3_dir character. Highest level S3 directory path. All files are uploaded under this.
    s3_dir = NULL,

    #' Initialize S3DirUploader
    #'
    #' @description
    #' Creates a new S3DirUploader instance of [R6][R6::R6Class] class.
    #'
    #' @param upload_id character. Unique identifier for this upload session.
    #' @param local_dir character. Path to the local directory to upload.
    #' @param bucket character. Name of the target S3 bucket.
    #' @param s3_dir character. S3 directory path to upload to.
    #' @param file_pattern character or NULL. Regex pattern passed to list.files() (default: NULL).
    #' @param filter_fn function or NULL. Function to further filter files for upload (default: NULL).
    #' @param name_fn function or NULL. Function to transform file names for S3 keys (default: NULL).
    #' @param s3_inst [paws.storage][paws.storage::s3] or NULL.
    #' An S3 client instance or if NULL one will be created (default: NULL).
    #' @param public logical. Whether files should be publicly accessible (default: FALSE).
    #' @param recursive logical. Whether to include subdirectories in upload (default: FALSE).
    #'
    #' @return A new S3DirUploader.
    initialize = \(
      upload_id,
      local_dir,
      bucket,
      s3_dir,
      file_pattern = NULL,
      filter_fn = NULL,
      name_fn = NULL,
      s3_inst = NULL,
      public = FALSE,
      recursive = FALSE
    ) {
      # fmt: skip
      stopifnot(
        "`s3_inst` is required to be NULL or a valid Paws S3 class" = (
          is.null(s3_inst) || validate_s3_class(s3_inst)
        ),
        "`local_dir` is required to be an existing directory path" = (
          dir.exists(local_dir) && length(local_dir) == 1
        ),
        "`pattern` is required to be NULL or a single character string" = (
          is.null(file_pattern) || (is.character(file_pattern) && length(file_pattern) == 1)
        ),
        "`filter_fn` is required to be NULL or a function" = (
          is.null(filter_fn) || is.function(filter_fn)
        ),
        "`name_fn` is required to be NULL or a function" = (
          is.null(name_fn) || is.function(name_fn)
        ),
        "`public` is required to be a logical vector" = (
          is.logical(public)
        ),
        "`recursive` is required to be a logical vector" = (
          is.logical(recursive)
        ),
        "`s3_dir` is required to be a single character string" = (
          is.character(s3_dir) && length(s3_dir) == 1
        )
      )

      private$s3_inst <- s3_inst %||% paws.storage::s3()
      private$.local_dir <- local_dir
      private$.pattern_ft <- file_pattern
      filter_fn <- filter_fn %||% identity # placeholder fn to return value if null
      private$.filter_fn <- filter_fn
      private$.files <- list.files(
        local_dir,
        pattern = file_pattern,
        full.names = TRUE,
        recursive = recursive
      ) |>
        filter_fn()
      private$.name_fn <- name_fn %||% \(x) basename(x)
      self$s3_dir <- s3_dir
      self$bucket <- bucket
      self$public <- public
      self$upload_id <- upload_id
      self$recursive <- recursive
    },
    #' @description
    #' Print class.
    print = \() {
      cat("<S3DirUploader>\n")
      cat("  ID:          ", self$upload_id, "\n")
      cat("  Bucket:      ", self$bucket, "\n")
      cat("  Public:      ", self$public, "\n")
      cat("  Local Dir:   ", private$.local_dir, "\n")
      cat("  S3 Dir:      ", self$s3_dir, "\n")
      cat("  Upload Date: ", private$.date, "\n")
      cat("  # Files:     ", length(private$.files), "\n")
      cat("  Complete:    ", private$.complete, "\n")
      cat("  Failed:      ", length(private$.failed), " files\n")
      cat("  S3 ACL:      ", private$.acl, "\n")
      cat("  File Pattern: ", private$.pattern_ft, "\n")
      invisible(self)
    },
    #' @description
    #' Test to see what the export (s3) paths will be before uploading using the name_fn on the files.
    #' @param x integer. Numbers of files to test.
    dry_run = \(x = 1) {
      files <- private$.files[c(1:x)]
      outputs <- sprintf(
        "s3://%s/%s/%s",
        self$bucket,
        self$s3_dir,
        private$.name_fn(files)
      )
      return(outputs)
    },
    #' @description
    #' Head local files that will be uploaded.
    #' @param x integer. Numbers of files to show.
    head = \(x = 5) {
      head(private$.files, x)
    },
    #' @description
    #' Validate the bucket and S3 connection are valid and permissions are correct.
    validate_con = \() {
      tryCatch(
        {
          result <- private$s3_inst$head_bucket(self$bucket)
          return(TRUE)
        },
        error = function(e) {
          message(sprintf("S3 validation failed:\n %s", e$message))
          return(FALSE)
        }
      )
    },
    #' @description
    #' Uploads all files to S3 sequentially. Validates connection first,
    #' then uploads each file with retry logic.
    #'
    #' @return List of upload results for each file.
    upload_files = \() {
      valid_s3 <- self$validate_con()
      if (!valid_s3) {
        stop("S3 connection not valid")
      }
      start_time <- Sys.time()
      s3_paths <- sprintf(
        "%s/%s",
        self$s3_dir,
        private$.name_fn(private$.files)
      )
      results_ls <- vector(mode = "list", length = length(private$.files))
      for (i in seq_along(private$.files)) {
        # faster than lapply when not parallel
        results_ls[[i]] <- s3_upload(
          local_path = private$.files[i],
          s3_key = s3_paths[i],
          private$s3_inst,
          self$bucket,
          max_tries = 3
        )
      }

      end_time <- Sys.time()
      private$.time <- difftime(end_time, start_time, units = "secs")
      private$.acl <- private$set_acl()

      private$.results <- results_ls
      print
      private$.failed <- Filter(\(x) isFALSE(x$success), results_ls)
      private$.complete <- length(private$.failed) == 0
      return(results_ls)
    },
    #' @description
    #' Uploads files to S3 using parallel processing for improved performance
    #' with large numbers of files.
    #'
    #' @param n_cores integer. Number of cores to use for parallel processing (default: 4).
    #' @param type character. Type of parallel backend: "multicore" or "multisession" (default: "multicore").
    #'
    #' @return List of upload results for each file.
    #'
    #' @details
    #' Requires the 'future' and 'future.apply' packages to be installed.
    upload_files_parallel = \(
      n_cores = 4,
      type = c("multisession", "multicore")
    ) {
      if (
        !requireNamespace("future", quietly = TRUE) ||
          !requireNamespace("future.apply", quietly = TRUE)
      ) {
        stop(
          "Packages 'future' and 'future.apply' must be installed for parallel uploading."
        )
      }
      valid_s3 <- self$validate_con()
      if (!valid_s3) {
        stop("S3 connection not valid")
      }
      type <- match.arg(type)
      start_time <- Sys.time()

      s3_paths <- sprintf(
        "%s/%s",
        self$s3_dir,
        private$.name_fn(private$.files)
      )
      future::plan(strategy = type, workers = n_cores)
      results_ls <- future.apply::future_lapply(
        seq_along(s3_paths),
        \(i) {
          s3_upload(
            local_path = private$.files[i],
            s3_key = s3_paths[i],
            private$s3_inst,
            self$bucket,
            max_tries = 3
          )
        },
        future.seed <- TRUE # fix the generated random numbers warning
      )
      future::plan("sequential")

      end_time <- Sys.time()
      private$.time <- difftime(end_time, start_time, units = "secs")
      private$.results <- results_ls
      private$.failed <- Filter(\(x) isFALSE(x$success), results_ls)
      private$.acl <- private$set_acl()
      private$.complete <- length(private$.failed) == 0
      return(results_ls)
    },
    #' @description
    #' Save a report of the upload results.
    #'
    #' @param path character. Path to save the report.
    save_report = \(path = NULL) {
      report_ls <- list(
        id = self$upload_id,
        bucket = self$bucket,
        public = self$public,
        local_dir = private$.local_dir,
        s3_dir = self$s3_dir,
        access = private$.acl,
        upload_date = private$.date,
        n_files = length(private$.files),
        completed = private$.complete,
        failed = private$.failed,
        time = format(private$.time), # convert to char as it is a difftime & doesn't parse
        file_pattern = private$.pattern_ft,
        filter = deparse1(private$.filter_fn)
      )
      save_path <- path %||%
        paste0(self$local_dir, "/", self$upload_id, "upload_report.json")
      jsonlite::write_json(
        report_ls,
        save_path,
        pretty = TRUE,
        auto_unbox = TRUE
      )
      print(sprintf("Report saved to: %s", save_path))
    }
  ),
  private = list(
    .files = NULL,
    .n_files = NULL,
    .failed = NULL,
    .results = NULL,
    .complete = FALSE,
    .time = NULL,
    .date = format(Sys.Date()),
    .local_dir = NULL,
    .pattern_ft = NULL,
    .name_fn = NULL,
    .filter_fn = NULL,
    s3_inst = NULL,
    .acl = NULL,
    set_acl = \() {
      if (self$public) {
        tryCatch(
          {
            set_uri_public(
              s3_uri = sprintf("s3://%s/%s", self$bucket, self$s3_dir),
              self$bucket,
              directory = TRUE,
              s3_inst = private$s3_inst
            )
            return("public")
          },
          error = function(e) {
            return(paste0("Setting ACL failed: ", e$message))
          }
        )
      } else {
        return("private")
      }
    }
  ),
  active = list(
    #' @field n_files integer. Read-only. Number of files that will be uploaded.
    n_files = \(x) {
      if (missing(x)) {
        length(private$.files)
      } else {
        stop("`$n_files` is read only", call. = FALSE)
      }
    },
    #' @field files character. Read-only. List of files that will be uploaded.
    files = \(x) {
      if (missing(x)) {
        private$.files
      } else {
        stop("`$files` is read only", call. = FALSE)
      }
    },
    #' @field failed list(character). Read-only. List of files that failed to upload.
    failed = \(x) {
      if (missing(x)) {
        private$.failed
      } else {
        stop("`$failed` is read only", call. = FALSE)
      }
    },
    #' @field time difftime. Read-only. Date of upload.
    time = \(x) {
      if (missing(x)) {
        private$.time
      } else {
        stop("`$time` is read only", call. = FALSE)
      }
    },
    #' @field date list(character). Read-only. Date of upload.
    date = \(x) {
      if (missing(x)) {
        private$.date
      } else {
        stop("`$date` is read only", call. = FALSE)
      }
    },
    #' @field local_dir character. The directory to be uploaded
    local_dir = \(x) {
      if (missing(x)) {
        private$.local_dir
      } else {
        stopifnot(dir.exists(x), length(x) == 1)
        private$.local_dir <- x
        private$.files <- list.files(
          x,
          pattern = private$.pattern_ft,
          full.names = TRUE,
          recursive = recursive
        )
      }
    },
    #' @field pattern_ft character. The pattern to select which files/types to upload.
    pattern_ft = \(x) {
      if (missing(x)) {
        private$.pattern_ft
      } else {
        stopifnot(is.null(x) || is.character(x))
        private$.pattern_ft <- x
        if (!is.null(private$.local_dir)) {
          private$.files <- list.files(
            private$.local_dir,
            pattern = x,
            full.names = TRUE,
            recursive = self$recursive
          ) |>
            private$.filter_fn()
        }
      }
    },
    #' @field name_fn function. Name transformation function.
    name_fn = \(x) {
      if (missing(x)) {
        if (!is.null(private$.files) && length(private$.files) > 0) {
          cat("Example transformation:\n")
          example_file <- private$.files[1]
          cat("Input: ", example_file, "\n")
          cat("Output:", private$.name_fn(example_file), "\n")
        }
        private$.name_fn
      } else {
        stopifnot(is.function(x))
        private$.name_fn <- x
      }
    },
    #' @field filter_fn function or NULL. Function to further filter files for upload (default: NULL).
    filter_fn = \(x) {
      if (missing(x)) {
        private$.filter_fn
      } else {
        stopifnot(is.function(x) || is.null(x))
        private$.filter_fn <- x
        private$.files <- list.files(
          private$.local_dir,
          pattern = x,
          full.names = TRUE,
          recursive = self$recursive
        ) |>
          private$.filter_fn()
      }
    }
  )
)

# # Tests
# t <- S3DirUploader$new(
#   upload_id = "test",
#   local_dir = "~/Downloads",
#   bucket = "my-bucket",
#   s3_dir = "test"
# )
# #
# t$dry_run(10)
# t$pattern_ft <- ".tif$|.parquet$"
# t$dry_run(10)
#
# t$name_fn <- function(x) {
#   paste0("new_name", "/", basename(x))
# }
#
# t$public
#
# t$dry_run()
