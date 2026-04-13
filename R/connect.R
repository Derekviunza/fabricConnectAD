#' Connect to Microsoft Fabric using Authentication
#'
#' Establishes a connection to Microsoft Fabric using various authentication methods
#' including Active Directory Interactive and username/password authentication.
#'
#' @param fabric_endpoint Fabric endpoint URL (without database)
#' @param database_name Database name to connect to (optional, defaults to "uzima_db_backup")
#' @param email User email for authentication
#' @param password User password (optional, not needed for Active Directory Interactive)
#' @param driver ODBC driver name (default: "ODBC Driver 18 for SQL Server")
#' @param port Server port (default: 1433)
#' @param timeout Connection timeout in seconds (default: 30)
#' @param store_credentials Whether to store credentials securely (default: FALSE)
#' @param authentication_method Authentication method ("ActiveDirectoryInteractive", "ActiveDirectoryPassword", or "Password")
#' @param prompt_if_missing Whether to prompt for missing info (default: TRUE)
#'
#' @return A DBI connection object
#' @export
fabric_connect_ad <- function(
  fabric_endpoint,
  database_name = NULL,
  email = NULL,
  password = NULL,
  driver = "ODBC Driver 18 for SQL Server",
  port = 1433,
  timeout = 30,
  store_credentials = FALSE,
  authentication_method = "ActiveDirectoryInteractive",
  prompt_if_missing = TRUE
) {
  
  if (missing(fabric_endpoint) || is.null(fabric_endpoint) || fabric_endpoint == "") {
    stop("fabric_endpoint is required and cannot be empty")
  }
  
  if (authentication_method == "ActiveDirectoryInteractive") {
    if (is.null(email) && prompt_if_missing) {
      email <- readline(prompt = "Enter email: ")
      if (email == "") stop("Email is required")
    }
    
  } else if (authentication_method %in% c("ActiveDirectoryPassword", "Password")) {
    if (is.null(email) && prompt_if_missing) {
      email <- readline(prompt = "Enter email: ")
      if (email == "") stop("Email is required")
    }
    
    if (is.null(password) && prompt_if_missing) {
      password <- get_pass_hidden("Enter password: ")
      if (password == "") stop("Password is required")
    }
    
  } else {
    stop("Invalid authentication_method. Use 'ActiveDirectoryInteractive', 'ActiveDirectoryPassword', or 'Password'")
  }
  
  tryCatch({
    con <- DBI::dbConnect(
      odbc::odbc(),
      Driver = driver,
      Server = fabric_endpoint,
      Database = database_name,
      Authentication = authentication_method,
      UID = email,
      PWD = password,
      Encrypt = "yes",
      TrustServerCertificate = "no",
      Port = port,
      Timeout = timeout
    )
    
    message("Successfully connected to Fabric ", ifelse(is.null(database_name), "endpoint", ifelse(database_name == "master", "endpoint", "Lakehouse")))
    return(con)
    
  }, error = function(e) {
    error_msg <- e$message
    if (grepl("Login failed", error_msg, ignore.case = TRUE)) {
      stop("Authentication failed. Check credentials and MFA status.\nOriginal error: ", error_msg, call. = FALSE)
    } else {
      stop("Failed to connect to Fabric. Check endpoint and ODBC driver.\nOriginal error: ", error_msg, call. = FALSE)
    }
  })
}

#' Connect to Microsoft Fabric using Web Authentication
#'
#' Opens a local web page for credential entry and connects to Fabric. 
#' Designed for environments (like VMs) where standard interactive auth fails.
#'
#' @export
fabric_connect_web <- function(
  fabric_endpoint,
  database_name = "uzima_db_backup",
  email = NULL,
  password = NULL,
  driver = "ODBC Driver 18 for SQL Server",
  port = 1433,
  timeout = 30,
  store_credentials = FALSE,
  authentication_method = "ActiveDirectoryPassword",
  web_port = 8765,
  web_timeout = 300,
  host = "127.0.0.1"
) {
  if (!requireNamespace("httpuv", quietly = TRUE)) stop("Package 'httpuv' is required.")
  if (!requireNamespace("DBI", quietly = TRUE)) stop("Package 'DBI' is required.")
  if (!requireNamespace("odbc", quietly = TRUE)) stop("Package 'odbc' is required.")

  # State container for the async response
  state <- new.env(parent = emptyenv())
  state$done <- FALSE
  state$connection <- NULL
  state$error <- NULL

  # Helper: Parse POST body
  parse_form_body <- function(req) {
    body_raw <- if (is.null(req$rook.input)) raw(0) else req$rook.input$read()
    body_str <- rawToChar(body_raw)
    if (!nzchar(body_str)) return(list())
    
    pairs <- strsplit(body_str, "&", fixed = TRUE)[[1]]
    out <- list()
    for (pair in pairs) {
      kv <- strsplit(pair, "=", fixed = TRUE)[[1]]
      key <- utils::URLdecode(gsub("\\+", " ", kv[1]))
      val <- if (length(kv) > 1) utils::URLdecode(gsub("\\+", " ", kv[2])) else ""
      out[[key]] <- val
    }
    out
  }

  # Server logic
  app <- list(
    call = function(req) {
      if (identical(req$REQUEST_METHOD, "GET")) {
        body <- sprintf('
          <html>
          <head><style>
            body{font-family:sans-serif; background:#f4f7f9; display:flex; justify-content:center; padding-top:50px;}
            .box{background:white; padding:30px; border-radius:8px; box-shadow:0 2px 10px rgba(0,0,0,0.1); width:350px;}
            input{display:block; width:100%%; margin:10px 0; padding:10px; border:1px solid #ccc; border-radius:4px;}
            button{width:100%%; padding:10px; background:#0078d4; color:white; border:none; border-radius:4px; cursor:pointer;}
          </style></head>
          <body><div class="box">
            <h2>Fabric Login</h2>
            <form method="POST" action="/connect">
              <input type="text" name="email" placeholder="Email" value="%s" required>
              <input type="password" name="password" placeholder="Password" required>
              <button type="submit">Connect to R</button>
            </form>
          </div></body></html>', if(is.null(email)) "" else email)
        return(list(status = 200L, headers = list("Content-Type" = "text/html"), body = body))
      }

      if (identical(req$REQUEST_METHOD, "POST") && identical(req$PATH_INFO, "/connect")) {
        formData <- parse_form_body(req)
        tryCatch({
          con <- DBI::dbConnect(
            odbc::odbc(),
            Driver = driver, Server = fabric_endpoint, Database = database_name,
            Authentication = authentication_method, UID = formData$email, PWD = formData$password,
            Encrypt = "yes", TrustServerCertificate = "no", Port = port, Timeout = timeout
          )
          state$connection <- con
          state$done <- TRUE
          return(list(status = 200L, headers = list("Content-Type" = "text/html"), 
                      body = "<h3>Success! You can close this tab.</h3>"))
        }, error = function(e) {
          return(list(status = 200L, headers = list("Content-Type" = "text/html"), 
                      body = paste("<h3>Failed</h3><pre>", e$message, "</pre><a href='/'>Retry</a>")))
        })
      }
    }
  )

  server <- httpuv::startServer(host, web_port, app)
  on.exit(httpuv::stopServer(server))
  
  url <- paste0("http://", host, ":", web_port)
  message("Opening browser for credentials: ", url)
  utils::browseURL(url)

  deadline <- Sys.time() + web_timeout
  while (!state$done && Sys.time() < deadline) {
    httpuv::service()
    Sys.sleep(0.1)
  }

  if (state$done && !is.null(state$connection)) {
    message("Successfully connected to Fabric.")
    return(state$connection)
  } else {
    stop("Connection timed out or failed.")
  }
}

# --- Credential Helpers ---

store_fabric_credentials <- function(email, password) {
  key_material <- paste0(Sys.getenv("USERNAME"), Sys.getenv("COMPUTERNAME"), "FABRIC_KEY")
  encryption_key <- digest::digest(key_material, algo = "sha256")
  password_bytes <- charToRaw(password)
  key_bytes <- charToRaw(encryption_key)
  key_repeated <- rep(key_bytes, length.out = length(password_bytes))
  encrypted_hex <- paste0(xor(password_bytes, key_repeated), collapse = "")
  
  Sys.setenv(FABRIC_EMAIL = email, FABRIC_ENCRYPTED_PASSWORD = encrypted_hex, FABRIC_KEY_SALT = substr(encryption_key, 1, 8))
  message("Credentials stored in environment variables.")
}

clear_fabric_credentials <- function() {
  Sys.unsetenv(c("FABRIC_EMAIL", "FABRIC_ENCRYPTED_PASSWORD", "FABRIC_KEY_SALT"))
  message("Stored credentials cleared.")
}

get_pass_hidden <- function(prompt = "Enter password: ") {
  # Note: Use askpass::askpass() if available for true masking
  password <- readline(prompt = prompt)
  if (password == "") stop("Password required.")
  return(password)
}

# --- Package Management ---

update_fabricConnectAD <- function(force = FALSE, version = "latest", branch = "master") {
  if (!requireNamespace("remotes", quietly = TRUE)) install.packages("remotes")
  repo_spec <- "Derekviunza/fabricConnectAD"
  if (version != "latest") repo_spec <- paste0(repo_spec, "@", version)
  else if (branch != "master") repo_spec <- paste0(repo_spec, "#", branch)
  
  remotes::install_github(repo_spec, force = force, upgrade = "always")
  message("Update process complete. Restart R to apply.")
}

fabric_disconnect <- function(con) {
  if (inherits(con, "DBIConnection")) {
    DBI::dbDisconnect(con)
    message("Disconnected.")
  }
}