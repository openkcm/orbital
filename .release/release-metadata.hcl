release {
  provider                = "github"
  url                     = "https://github.com/openkcm/orbital/releases"
  changelog               = "CHANGELOG.md"
  tag_prefix              = "v"
  release_name_template   = "Release {{.Version}}"
  assets = [
    "*"
  ]
}
