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


#' Upload a file to S3 with retry and multipart support
#'
#' Uploads a file to an S3 bucket, using multipart upload for large files and retrying on failure.
#'
#' @param local_path character. Path to the local file.
#' @param s3_key character. Key for the object in the S3 bucket.
#' @param s3 [paws.storage][paws.storage::s3] An initialized S3 client object.
#' @param bucket character. Target S3 bucket name.
#' @param max_tries integer. Maximum number of upload attempts.
#' @return list. A list of upload details and result.
#' @export
s3_upload <- function(local_path, s3_key, s3, bucket, max_tries = 3) {
  if (!file.exists(local_path)) stop("File not found: ", local_path)
  file_size <- file.info(local_path)$size
  for (i in seq_len(max_tries)) {
    success <- tryCatch(
      {
        if (file_size > 5 * 2^20) {
          s3_multipart(local_path, s3_key, s3, bucket, 5)
        } else {
          con <- file(local_path, "rb")
          on.exit(close(con))
          s3$put_object(
            Body = con,
            Bucket = bucket,
            Key = s3_key
          )
        }
      },
      error = function(e) {
        FALSE
      }
    )
    if (success) {
      return(TRUE)
      break
    }
  }
  return(list(local = local_path, s3 = s3_key, success = success))
}

#' Perform multipart upload to S3
#'
#' Uploads a large file to an S3 bucket using the multipart upload API. Aborts the upload on failure.
#'
#' @param local_path character. Path to the local file.
#' @param s3_key character. Key for the object in the S3 bucket.
#' @param s3 [paws.storage][paws.storage::s3] An initialized S3 client object.
#' @param bucket character. Target S3 bucket name.
#' @param part_mb numeric. Part size in megabytes (minimum 5 MB).
#' @return list. Result of the complete multipart upload request.
#' @export
s3_multipart <- function(local_path, s3_key, s3, bucket, part_mb = 5) {
  stopifnot("Part size cannot be less than 5 MB" = part_mb >= 5)
  part_size <- part_mb * 2^20

  upload <- s3$create_multipart_upload(Bucket = bucket, Key = s3_key)
  upload_id <- upload$UploadId

  con <- file(local_path, "rb")
  on.exit(close(con), add = TRUE)

  parts <- list()
  part_number <- 1
  resp <- NULL

  # Ensure we abort the multipart upload if anything fails
  on.exit(
    {
      if (is.null(resp) || inherits(resp, "try-error")) {
        message("Aborting failed multipart upload...")
        try(
          s3$abort_multipart_upload(
            Bucket = bucket,
            Key = s3_key,
            UploadId = upload_id
          ),
          silent = TRUE
        )
      }
    },
    add = TRUE
  )

  resp <- try({
    repeat {
      part_data <- readBin(con, "raw", n = part_size)
      if (length(part_data) == 0) break

      part_resp <- s3$upload_part(
        Body = part_data,
        Bucket = bucket,
        Key = s3_key,
        PartNumber = part_number,
        UploadId = upload_id
      )

      parts[[part_number]] <- list(
        ETag = part_resp$ETag,
        PartNumber = part_number
      )

      part_number <- part_number + 1
    }

    s3$complete_multipart_upload(
      Bucket = bucket,
      Key = s3_key,
      MultipartUpload = list(Parts = parts),
      UploadId = upload_id
    )
  })

  return(resp)
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
    #' Initialize S3DirUploader
    #'
    #' @description
    #' Creates a new S3DirUploader instance of [R6][R6::R6Class] class.
    #'
    #' @param upload_id character. Unique identifier for this upload session.
    #' @param local_dir character. Path to the local directory to upload.
    #' @param bucket character. Name of the target S3 bucket.
    #' @param pattern_ft character or NULL. Regex pattern to filter files (default: NULL).
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
      pattern_ft = NULL,
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
        "`pattern_ft` is required to be NULL or a single character string" = (
          is.null(pattern_ft) || (is.character(pattern_ft) && length(pattern_ft) == 1)
        ),
        "`name_fn` is required to be NULL or a function" = (
          is.null(name_fn) || is.function(name_fn)
        ),
        "`public` is required to be a logical vector" = (
          is.logical(public)
        ),
        "`recursive` is required to be a logical vector" = (
          is.logical(recursive)
        )
      )

      private$s3_inst <- s3_inst %||% paws.storage::s3()
      private$.local_dir <- local_dir
      private$.pattern_ft <- pattern_ft
      private$.files <- list.files(
        local_dir,
        pattern = pattern_ft,
        full.names = TRUE,
        recursive = recursive
      )
      private$.name_fn <- name_fn %||% \(x) x
      self$bucket <- bucket
      self$public <- public
      self$upload_id <- upload_id
    },
    #' @description
    #' Print class.
    print = \() {
      cat("<S3DirUploader>\n")
      cat("  ID:          ", self$upload_id, "\n")
      cat("  Bucket:      ", self$bucket, "\n")
      cat("  Public:      ", self$public, "\n")
      cat("  Local Dir:   ", private$.local_dir, "\n")
      cat("  Upload Date: ", private$.date, "\n")
      cat("  # Files:     ", private$.n_files)
      cat("  Complete:    ", private$.complete, "\n")
      cat("  Failed:      ", length(private$.failed), " files\n")
      invisible(self)
    },
    #' @description
    #' Test to see what the export (s3) paths will be before uploading using the name_fn on the files.
    #' @param x integer. Numbers of files to test.
    dry_run = \(x = 1) {
      files <- private$.files[c(1:x)]
      outputs <- sprintf("s3://%s/%s", self$bucket, private$.name_fn(files))
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
      valid_s3 <- self$validate_con
      if (!valid_s3) {
        stop("S3 connection not valid")
      }
      start_time <- Sys.time()
      s3_paths <- sprintf(
        "s3://%s/%s",
        self$bucket,
        private$.name_fn(private$.files)
      )
      results_ls <- vector(mode = "list", length = private$.n_files)
      for (i in seq_len(private$.n_files)) {
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

      private$.results <- results_ls
      private$.failed <- Filter(\(x) isFALSE(x$success), results_ls)
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
      type = c("multicore", "multisession")
    ) {
      if (
        !requireNamespace("future", quietly = TRUE) ||
          !requireNamespace("future.apply", quietly = TRUE)
      ) {
        stop(
          "Packages 'future' and 'future.apply' must be installed for parallel uploading."
        )
      }
      valid_s3 <- self$validate_con
      if (!valid_s3) {
        stop("S3 connection not valid")
      }
      type <- match.arg(type)
      start_time <- Sys.time()

      s3_paths <- sprintf(
        "s3://%s/%s",
        self$bucket,
        private$.name_fn(private$.files)
      )
      future::plan(strategy = type, workers = n_cores)
      results_ls <- future.apply::future_lapply(seq_along(x), \(i) {
        s3_upload(
          local_path = private$.files[i],
          s3_key = s3_paths[i],
          private$s3_inst,
          self$bucket,
          max_tries = 3
        )
      })
      future::plan("sequential")

      end_time <- Sys.time()
      private$.time <- difftime(end_time, start_time, units = "secs")
      private$.results <- results_ls
      private$.failed <- Filter(\(x) isFALSE(x$success), results_ls)
      return(results_ls)
    }
  ),
  private = list(
    .n_files = NULL,
    .files = NULL,
    .failed = NULL,
    .complete = FALSE,
    .time = NULL,
    .date = format(Sys.Date()),
    .local_dir = NULL,
    .pattern_ft = NULL,
    .name_fn = NULL,
    s3_inst = NULL,
    set_acl = \() {
      stop("This function needs to be written") # TODO: add in acl function
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
            recursive = recursive
          )
        }
      }
    },
    #' @field name_fn function. Name transformation function.
    name_fn = \(x) {
      if (missing(x)) {
        if (!is.null(private$.files) && length(private$.files) > 0) {
          cat("Example transformation:\n")
          example_file <- basename(private$.files[1])
          cat("Input: ", example_file, "\n")
          cat("Output:", private$.name_fn(example_file), "\n")
        }
        private$.name_fn
      } else {
        stopifnot(is.function(x))
        private$.name_fn <- x
      }
    }
  )
)
