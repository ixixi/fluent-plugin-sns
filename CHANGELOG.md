## Changelog

### 3.0.0 (unreleased)

- Use AWS SDK v2: this should work the same as with AWS SDK v1, the only
  IMPORTANT change is that `sns_endpoint` has been replaced by `sns_region`
  (defaults to 'ap-northeast-1').
- Add support for SNS Message attributes.
