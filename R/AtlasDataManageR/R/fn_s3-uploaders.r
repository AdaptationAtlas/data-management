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
  if (!file.exists(local_path)) stop(paste0("File not found: ", local_path))
  file_size <- file.info(local_path)$size
  for (i in seq_len(max_tries)) {
    success <- tryCatch(
      {
        if (file_size > 5 * 2^20) {
          s3_multipart(local_path, s3_key, s3, bucket, 5)
        } else {
          s3$put_object(
            Body = local_path,
            Bucket = bucket,
            Key = s3_key
          )
        }
        TRUE
      },
      error = function(e) {
        message("Upload attempt failed: ", conditionMessage(e))
        FALSE
      }
    )
    if (success) {
      return(list(local = local_path, s3 = s3_key, success = TRUE))
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
