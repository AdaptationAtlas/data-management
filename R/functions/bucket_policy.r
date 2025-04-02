#####
# Title: AWS S3 Bucket Policy Functions
# Authors: Brayden Youngberg, Pete Steward
# Description: Functions for working with AWS S3 bucket policies
#####



get_bucket_policy <- function(bucket, s3_instance = NULL) {
  if (is.null(s3_instance)) {
    s3_instance <- paws.storage::s3()
  }
  s3_policy <- tryCatch(
    {
      s3_instance$get_bucket_policy(Bucket = bucket)$Policy
    }, error = function(e) {
      stop("Error retrieving bucket policy: ", e)
    }
  )
  policy_ls <- jsonlite::parse_json(s3_policy)
  return(policy_ls)
}

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
      prefixes <- tryCatch({
        unlist(condition$StringLike$`s3:prefix`)
        }, error = function(e) {
          warning(sprintf("No file prefixes found in policy condition for %s", permissions[i]))
          return(NULL)
        }
      )
      if (!is.null(prefixes)) {
        files <- paste0(arn, "/", prefixes)
        policy_df <- data.frame(permission = permissions[i],
			       action = actions[i],
			       effect = effects[i],
                               file = files,
                               stringsAsFactors = FALSE)
	policy_df_list[[i]] <- policy_df
      }
    } else {
      files <- unlist(statement[[i]]$Resource)
      policy_df <- data.frame(permission = permissions[i],
			      action = actions[i],
			      effect = effects[i],
                              file = files,
                              stringsAsFactors = FALSE)
      policy_df_list[[i]] <- policy_df
    }
  }
  policy_df <- do.call(rbind, policy_df_list)
  policy_df$file <- gsub("arn:aws:s3:::", "", policy_df$file)
  return(policy_df)
}

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

  if(gsub('/|s3:|\\*', "", s3_uri) == bucket) {
    stop(
      paste("Setting full bucket to public is not allowed with this function to prevent accidents,",
            "please manually update the policy if you require this."
      )
    )
  }

  policy_ls <- get_bucket_policy(bucket, s3_instance)

  policy_df <- file_policy_df(policy_ls)
  current_pub_files <- unique(subset(policy_df, permission %in% c(getID, listID), select = file))
  current_regex <- paste0("^", current_pub_files$file)
  s3_uri_clean <- gsub("s3://", "", s3_uri)
  matches <-  stringi::stri_detect(s3_uri_clean, regex = current_regex)
  if (any(matches)) {
    print("The current policy already allows public access to the specified files.")
    access_from <- paste0(current_pub_files[matches, 'file'], collapse = ", ")
    stop(
      sprintf("Access is granted under this resource: %s", access_from)
    )
  }

  # Backup the current policy and store it in the bucket
  tmp <- tempdir()
  tmp_dir <- file.path(tmp, "s3_policy")
  if (!dir.exists(tmp_dir)) dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE))
  jsonlite::write_json(policy_ls, file.path(tmp_dir, "previous_policy.json"),
                       pretty = TRUE, auto_unbox = TRUE)
  if (!test) {
  s3_instance$put_object(Bucket = bucket, 
                     Key = ".bucket_policy/previous_policy.json",
                     Body = file.path(tmp_dir, "previous_policy.json"))
  }

  dir_wildcard  <- ifelse(directory, "/*", "")
  s3_uri_clean <- paste0(s3_uri_clean, dir_wildcard)

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

  new_policy <- jsonlite::toJSON(policy_ls, pretty = T, auto_unbox = T)
  if (!test) {
  s3_instance$put_bucket_policy(Bucket = bucket, Policy = new_policy)
  jsonlite::write_json(policy_ls, file.path(tmp_dir, 'current_policy.json'), 
                       pretty = T, auto_unbox = T)
  s3_instance$put_object(Bucket = bucket, 
                     Key = '.bucket_policy/current_policy.json',
                     Body = file.path(tmp_dir, 'current_policy.json'))
  }
  return(new_policy)
}

