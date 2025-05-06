#####
# Title: AWS S3 Bucket Policy Functions
# Authors: Brayden Youngberg, Pete Steward
# Description: Functions for working with AWS S3 bucket policies
#####

#' Retrieve an AWS S3 Bucket Policy
#'
#' Retrieves and parses the policy for a specified AWS S3 bucket. The function
#' handles the AWS API call and converts the JSON policy to an R list structure.
#'
#' @param bucket Character string specifying the name of the S3 bucket
#' @param s3_instance Optional AWS S3 client instance. If NULL, a new instance is created using paws.storage::s3().
#'
#' @return A list containing the parsed bucket policy with all statements and conditions
#' @export
#'
#' @examples
#' \dontrun{
#' # Get policy for a bucket named "my-bucket"
#' policy <- get_bucket_policy("my-bucket")
#' 
#' # Use an existing S3 client
#' s3_client <- paws.storage::s3()
#' policy <- get_bucket_policy("my-bucket", s3_client)
#' }
#'
#' @note The function will stop with an error message if it fails to retrieve the bucket policy.
#'
#' @importFrom paws.storage s3
#' @importFrom jsonlite parse_json
get_bucket_policy <- function(bucket, s3_instance = NULL) {
  if (is.null(s3_instance)) {
    s3_instance <- paws.storage::s3()
  }
  s3_policy <- tryCatch(
    {
      s3_instance$get_bucket_policy(Bucket = bucket)$Policy
    },
    error = function(e) {
      stop("Error retrieving bucket policy: ", e)
    }
  )
  policy_ls <- jsonlite::parse_json(s3_policy)
  return(policy_ls)
}


#' Convert S3 Bucket Policy to a Data Frame
#'
#' Converts an AWS S3 bucket policy list into a data frame format for easier analysis
#' and manipulation. The function extracts permissions (Sid), actions, effects, and file paths
#' from the policy statements. It handles both conditional and non-conditional policy rules.
#'
#' @param policy_ls A list containing an S3 bucket policy (as returned by get_bucket_policy)
#'
#' @return A data frame with the following columns:
#'   \item{permission}{The policy statement identifier (Sid)}
#'   \item{action}{The AWS action(s) allowed or denied}
#'   \item{effect}{The effect of the policy (Allow/Deny)}
#'   \item{file}{The S3 resource path (with "arn:aws:s3:::" prefix removed)}
#' @export
#'
#' @examples
#' \dontrun{
#' policy <- get_bucket_policy("my-bucket")
#' policy_df <- file_policy_df(policy)
#' head(policy_df)
#' 
#' # Filter for specific permissions
#' subset(policy_df, permission == "AllowPublicGet")
#' }
#'
file_policy_df <- function(policy_ls) {
  statement <- policy_ls$Statement
  permissions <- sapply(statement, `[[`, "Sid")
  actions <- sapply(statement, `[[`, "Action")
  effects <- sapply(statement, `[[`, "Effect")
  num_rules <- length(statement)
  policy_df_list <- vector(mode = "list", length = num_rules)
  for (i in 1:num_rules) {
    keys <- names(statement[[i]])
    if ("Condition" %in% keys) {
      condition <- statement[[i]]$Condition
      arn <- statement[[i]]$Resource
      prefixes <- tryCatch(
        {
          unlist(condition$StringLike$`s3:prefix`)
        },
        error = function(e) {
          warning(sprintf("No file prefixes found in policy condition for %s", permissions[i]))
          return(NULL)
        }
      )
      if (!is.null(prefixes)) {
        files <- paste0(arn, "/", prefixes)
        policy_df <- data.frame(
          permission = permissions[i],
          action = actions[i],
          effect = effects[i],
          file = files,
          stringsAsFactors = FALSE
        )
        policy_df_list[[i]] <- policy_df
      }
    } else {
      files <- unlist(statement[[i]]$Resource)
      policy_df <- data.frame(
        permission = permissions[i],
        action = actions[i],
        effect = effects[i],
        file = files,
        stringsAsFactors = FALSE
      )
      policy_df_list[[i]] <- policy_df
    }
  }
  policy_df <- do.call(rbind, policy_df_list)
  policy_df$file <- gsub("arn:aws:s3:::", "", policy_df$file)
  return(policy_df)
}

#' Make an S3 URI Publicly Accessible
#'
#' Updates an AWS S3 bucket policy to make a specified URI publicly accessible.
#' The function adds the URI to both get and list permissions in the bucket policy.
#' Safety features prevent accidental public exposure of the entire bucket.
#' 
#' The function creates backups of the policy before and after making changes,
#' storing these in the .bucket_policy/ directory of the bucket.
#'
#' @param s3_uri Character string specifying the S3 URI to make public (e.g., "s3://bucket-name/path/to/files/")
#' @param bucket Character string specifying the bucket name (default: "digital-atlas")
#' @param directory Logical indicating if the URI should be treated as a directory by appending "/*" (default: TRUE)
#' @param test Logical indicating whether to run in test mode without applying changes (default: FALSE)
#' @param s3_instance Optional AWS S3 client instance. If NULL, a new instance is created using paws.storage::s3().
#' @param getID Character string specifying the policy statement ID for GET permissions (default: "AllowPublicGet")
#' @param listID Character string specifying the policy statement ID for LIST permissions (default: "AllowPublicList")
#'
#' @return The updated policy as a JSON string
#' @export
#'
#' @examples
#' \dontrun{
#' # Make a directory public
#' set_uri_public("s3://my-bucket/public-data/")
#'
#' # Test without applying changes
#' set_uri_public("s3://my-bucket/public-data/", test = TRUE)
#'
#' # Make a single file public
#' set_uri_public("s3://my-bucket/path/to/file.csv", directory = FALSE)
#' }
#'
#' @note This function requires both AllowPublicGet and AllowPublicList statements to
#' already exist in the bucket policy. It will not create these statements if they don't exist.
#'
#' @importFrom paws.storage s3
#' @importFrom jsonlite parse_json toJSON write_json
#' @importFrom stringi stri_detect
set_uri_public <- function(
    s3_uri,
    bucket = "digital-atlas",
    directory = TRUE,
    test = FALSE,
    s3_instance = NULL,
    getID = "AllowPublicGet",
    listID = "AllowPublicList") {
  if (is.null(s3_instance)) {
    s3_instance <- paws.storage::s3()
  }

  if (gsub("/|s3:|\\*", "", s3_uri) == bucket) {
    stop(
      paste(
        "Setting full bucket to public is not allowed with this function to prevent accidents,",
        "please manually update the policy if you require this."
      )
    )
  }

  tryCatch({
    policy_ls <- get_bucket_policy(bucket, s3_instance)
  }, error = function(e) {
    stop(paste("Failed to retrieve bucket policy:", e$message))
  })

  policy_ls <- get_bucket_policy(bucket, s3_instance)

  policy_df <- file_policy_df(policy_ls)
  current_pub_files <- unique(subset(policy_df, permission %in% c(getID, listID), select = file))
  current_regex <- paste0("^", current_pub_files$file)
  s3_uri <- trimws(s3_uri)
  s3_uri_clean <- gsub("s3://", "", s3_uri)
  matches <- stringi::stri_detect(s3_uri_clean, regex = current_regex)
  if (any(matches)) {
    cat("The current policy already allows public access to the specified files. \n")
    access_from <- paste0(current_pub_files[matches, "file"], collapse = ", ")
    stop(
      sprintf("Access is granted under this resource: %s", access_from)
    )
  }

  tmp <- tempdir()
  tmp_dir <- file.path(tmp, "s3_policy")
  if (!dir.exists(tmp_dir)) dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE))

  # Backup the current policy and store it in the bucket
  if (!test) {
    jsonlite::write_json(policy_ls, file.path(tmp_dir, "previous_policy.json"),
      pretty = TRUE, auto_unbox = TRUE
    )
    s3_instance$put_object(
      Bucket = bucket,
      Key = ".bucket_policy/previous_policy.json",
      Body = file.path(tmp_dir, "previous_policy.json")
    )
  }

  if (directory) {
    s3_uri_clean <- paste0(sub("/?$", "/", s3_uri_clean), "*")
  }
  s3_arn <- paste0("arn:aws:s3:::", s3_uri_clean)
  s3_path <- gsub(paste0(bucket, "/"), "", s3_uri_clean)


  policy_ls$Statement <- lapply(policy_ls$Statement, function(statement) {
    case <- statement$Sid

    if (case == getID) {
      statement$Resource <- unique(c(statement$Resource, s3_arn))
    } else if (case == listID) {
      statement$Condition$StringLike$`s3:prefix` <- unique(
        c(statement$Condition$StringLike$`s3:prefix`, s3_path)
      )
    }
    return(statement)
  })

  new_policy <- jsonlite::toJSON(policy_ls, pretty = TRUE, auto_unbox = TRUE)
  if (!test) {
    s3_instance$put_bucket_policy(Bucket = bucket, Policy = new_policy)
    jsonlite::write_json(policy_ls, file.path(tmp_dir, "current_policy.json"),
      pretty = TRUE, auto_unbox = TRUE
    )
    s3_instance$put_object(
      Bucket = bucket,
      Key = ".bucket_policy/current_policy.json",
      Body = file.path(tmp_dir, "current_policy.json")
    )
  }
  return(new_policy)
}
