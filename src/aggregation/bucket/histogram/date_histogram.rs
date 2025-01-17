use serde::{Deserialize, Serialize};

use super::{HistogramAggregation, HistogramBounds};
use crate::aggregation::AggregationError;

/// DateHistogramAggregation is similar to `HistogramAggregation`, but it can only be used with date
/// type.
///
/// Currently only **fixed time** intervals are supported. Calendar-aware time intervals are not
/// supported.
///
/// Like the histogram, values are rounded down into the closest bucket.
///
/// For this calculation all fastfield values are converted to f64.
///
/// # Limitations/Compatibility
/// Only fixed time intervals are supported.
///
/// # JSON Format
/// ```json
/// {
///     "prices": {
///         "date_histogram": {
///             "field": "price",
///             "fixed_interval": "30d"
///         }
///     }
/// }
/// ```
///
/// Response
/// See [`BucketEntry`](crate::aggregation::agg_result::BucketEntry)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DateHistogramAggregationReq {
    #[doc(hidden)]
    /// Only for validation
    interval: Option<String>,
    #[doc(hidden)]
    /// Only for validation
    date_interval: Option<String>,
    /// The field to aggregate on.
    pub field: String,
    /// The format to format dates.
    pub format: Option<String>,
    /// The interval to chunk your data range. Each bucket spans a value range of
    /// [0..fixed_interval). Accepted values
    ///
    /// Fixed intervals are configured with the `fixed_interval` parameter.
    /// In contrast to calendar-aware intervals, fixed intervals are a fixed number of SI units and
    /// never deviate, regardless of where they fall on the calendar. One second is always
    /// composed of 1000ms. This allows fixed intervals to be specified in any multiple of the
    /// supported units. However, it means fixed intervals cannot express other units such as
    /// months, since the duration of a month is not a fixed quantity. Attempting to specify a
    /// calendar interval like month or quarter will return an Error.
    ///
    /// The accepted units for fixed intervals are:
    /// * `ms`: milliseconds
    /// * `s`: seconds. Defined as 1000 milliseconds each.
    /// * `m`: minutes. Defined as 60 seconds each (60_000 milliseconds).
    /// * `h`: hours. Defined as 60 minutes each (3_600_000 milliseconds).
    /// * `d`: days. Defined as 24 hours (86_400_000 milliseconds).
    ///
    /// Fractional time values are not supported, but you can address this by shifting to another
    /// time unit (e.g., `1.5h` could instead be specified as `90m`).
    pub fixed_interval: String,
    /// Intervals implicitly defines an absolute grid of buckets `[interval * k, interval * (k +
    /// 1))`.
    pub offset: Option<String>,
    /// The minimum number of documents in a bucket to be returned. Defaults to 0.
    pub min_doc_count: Option<u64>,
    /// Limits the data range to `[min, max]` closed interval.
    ///
    /// This can be used to filter values if they are not in the data range.
    ///
    /// hard_bounds only limits the buckets, to force a range set both extended_bounds and
    /// hard_bounds to the same range.
    ///
    /// Needs to be provided as timestamp in microseconds precision.
    ///
    /// ## Example
    /// ```json
    /// {
    ///     "sales_over_time": {
    ///        "date_histogram": {
    ///            "field": "dates",
    ///            "interval": "1d",
    ///            "hard_bounds": {
    ///                "min": 0,
    ///                "max": 1420502400000000
    ///            }
    ///        }
    ///    }
    /// }
    /// ```
    pub hard_bounds: Option<HistogramBounds>,
    /// Can be set to extend your bounds. The range of the buckets is by default defined by the
    /// data range of the values of the documents. As the name suggests, this can only be used to
    /// extend the value range. If the bounds for min or max are not extending the range, the value
    /// has no effect on the returned buckets.
    ///
    /// Cannot be set in conjunction with min_doc_count > 0, since the empty buckets from extended
    /// bounds would not be returned.
    pub extended_bounds: Option<HistogramBounds>,

    /// Whether to return the buckets as a hash map
    #[serde(default)]
    pub keyed: bool,
}

impl DateHistogramAggregationReq {
    pub(crate) fn to_histogram_req(&self) -> crate::Result<HistogramAggregation> {
        self.validate()?;
        Ok(HistogramAggregation {
            field: self.field.to_string(),
            interval: parse_into_microseconds(&self.fixed_interval)? as f64,
            offset: self
                .offset
                .as_ref()
                .map(|offset| parse_offset_into_microseconds(offset))
                .transpose()?
                .map(|el| el as f64),
            min_doc_count: self.min_doc_count,
            hard_bounds: None,
            extended_bounds: None,
            keyed: self.keyed,
        })
    }

    fn validate(&self) -> crate::Result<()> {
        if self.interval.is_some() {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "`interval` parameter {:?} in date histogram is unsupported, only \
                 `fixed_interval` is supported",
                self.interval
            )));
        }
        if self.format.is_some() {
            return Err(crate::TantivyError::InvalidArgument(
                "format parameter on date_histogram is unsupported".to_string(),
            ));
        }

        if self.date_interval.is_some() {
            return Err(crate::TantivyError::InvalidArgument(
                "date_interval in date histogram is unsupported, only `fixed_interval` is \
                 supported"
                    .to_string(),
            ));
        }

        parse_into_microseconds(&self.fixed_interval)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
/// Errors when parsing the fixed interval for `DateHistogramAggregationReq`.
pub enum DateHistogramParseError {
    /// Unit not recognized in passed String
    #[error("Unit not recognized in passed String {0:?}")]
    UnitNotRecognized(String),
    /// Number not found in passed String
    #[error("Number not found in passed String {0:?}")]
    NumberMissing(String),
    /// Unit not found in passed String
    #[error("Unit not found in passed String {0:?}")]
    UnitMissing(String),
    /// Offset invalid
    #[error("passed offset is invalid {0:?}")]
    InvalidOffset(String),
}

fn parse_offset_into_microseconds(input: &str) -> Result<i64, AggregationError> {
    let is_sign = |byte| &[byte] == b"-" || &[byte] == b"+";
    if input.is_empty() {
        return Err(DateHistogramParseError::InvalidOffset(input.to_string()).into());
    }

    let has_sign = is_sign(input.as_bytes()[0]);
    if has_sign {
        let (sign, input) = input.split_at(1);
        let val = parse_into_microseconds(input)?;
        if sign == "-" {
            Ok(-val)
        } else {
            Ok(val)
        }
    } else {
        parse_into_microseconds(input)
    }
}

fn parse_into_microseconds(input: &str) -> Result<i64, AggregationError> {
    let split_boundary = input
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let (number, unit) = input.split_at(split_boundary);
    if number.is_empty() {
        return Err(DateHistogramParseError::NumberMissing(input.to_string()).into());
    }
    if unit.is_empty() {
        return Err(DateHistogramParseError::UnitMissing(input.to_string()).into());
    }
    let number: i64 = number
        .parse()
        // Technically this should never happen, but there was a bug
        // here and being defensive does not hurt.
        .map_err(|_err| DateHistogramParseError::NumberMissing(input.to_string()))?;

    let multiplier_from_unit = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60 * 1000,
        "h" => 60 * 60 * 1000,
        "d" => 24 * 60 * 60 * 1000,
        _ => return Err(DateHistogramParseError::UnitNotRecognized(unit.to_string()).into()),
    };

    Ok(number * multiplier_from_unit * 1000)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::exec_request;
    use crate::indexer::NoMergePolicy;
    use crate::schema::{Schema, FAST};
    use crate::Index;

    #[test]
    fn test_parse_into_microseconds() {
        assert_eq!(parse_into_microseconds("1m").unwrap(), 60_000_000);
        assert_eq!(parse_into_microseconds("2m").unwrap(), 120_000_000);
        assert_eq!(
            parse_into_microseconds("2y").unwrap_err(),
            DateHistogramParseError::UnitNotRecognized("y".to_string()).into()
        );
        assert_eq!(
            parse_into_microseconds("2000").unwrap_err(),
            DateHistogramParseError::UnitMissing("2000".to_string()).into()
        );
        assert_eq!(
            parse_into_microseconds("ms").unwrap_err(),
            DateHistogramParseError::NumberMissing("ms".to_string()).into()
        );
    }

    #[test]
    fn test_parse_offset_into_microseconds() {
        assert_eq!(parse_offset_into_microseconds("1m").unwrap(), 60_000_000);
        assert_eq!(parse_offset_into_microseconds("+1m").unwrap(), 60_000_000);
        assert_eq!(parse_offset_into_microseconds("-1m").unwrap(), -60_000_000);
        assert_eq!(parse_offset_into_microseconds("2m").unwrap(), 120_000_000);
        assert_eq!(parse_offset_into_microseconds("+2m").unwrap(), 120_000_000);
        assert_eq!(parse_offset_into_microseconds("-2m").unwrap(), -120_000_000);
        assert_eq!(parse_offset_into_microseconds("-2ms").unwrap(), -2_000);
        assert_eq!(
            parse_offset_into_microseconds("2y").unwrap_err(),
            DateHistogramParseError::UnitNotRecognized("y".to_string()).into()
        );
        assert_eq!(
            parse_offset_into_microseconds("2000").unwrap_err(),
            DateHistogramParseError::UnitMissing("2000".to_string()).into()
        );
        assert_eq!(
            parse_offset_into_microseconds("ms").unwrap_err(),
            DateHistogramParseError::NumberMissing("ms".to_string()).into()
        );
    }

    #[test]
    fn test_parse_into_milliseconds_do_not_accept_non_ascii() {
        assert!(parse_into_microseconds("１m").is_err());
    }

    pub fn get_test_index_from_docs(
        merge_segments: bool,
        segment_and_docs: &[Vec<&str>],
    ) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("date", FAST);
        schema_builder.add_text_field("text", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        {
            let mut index_writer = index.writer_with_num_threads(1, 30_000_000)?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            for values in segment_and_docs {
                for doc_str in values {
                    let doc = schema.parse_document(doc_str)?;
                    index_writer.add_document(doc)?;
                }
                // writing the segment
                index_writer.commit()?;
            }
        }
        if merge_segments {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            if segment_ids.len() > 1 {
                let mut index_writer = index.writer_for_tests()?;
                index_writer.merge(&segment_ids).wait()?;
                index_writer.wait_merging_threads()?;
            }
        }

        Ok(index)
    }

    #[test]
    fn histogram_test_date_force_merge_segments() -> crate::Result<()> {
        histogram_test_date_merge_segments(true)
    }

    #[test]
    fn histogram_test_date() -> crate::Result<()> {
        histogram_test_date_merge_segments(false)
    }
    fn histogram_test_date_merge_segments(merge_segments: bool) -> crate::Result<()> {
        let docs = vec![
            vec![r#"{ "date": "2015-01-01T12:10:30Z", "text": "aaa" }"#],
            vec![r#"{ "date": "2015-01-01T11:11:30Z", "text": "bbb" }"#],
            vec![r#"{ "date": "2015-01-02T00:00:00Z", "text": "bbb" }"#],
            vec![r#"{ "date": "2015-01-06T00:00:00Z", "text": "ccc" }"#],
        ];

        let index = get_test_index_from_docs(merge_segments, &docs)?;
        // 30day + offset
        let elasticsearch_compatible_json = json!(
            {
              "sales_over_time": {
                "date_histogram": {
                  "field": "date",
                  "fixed_interval": "30d",
                  "offset": "-4d"
                }
              }
            }
        );

        let agg_req: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        let res = exec_request(agg_req, &index)?;
        let expected_res = json!({
          "sales_over_time" : {
          "buckets" : [
            {
              "key_as_string" : "2015-01-01T00:00:00Z",
              "key" : 1420070400000000.0,
              "doc_count" : 4
            }
          ]
        }
        });
        assert_eq!(res, expected_res);

        // 30day + offset + sub_agg
        let elasticsearch_compatible_json = json!(
            {
              "sales_over_time": {
                "date_histogram": {
                  "field": "date",
                  "fixed_interval": "30d",
                  "offset": "-4d"
                },
                "aggs": {
                    "texts": {
                        "terms": {"field": "text"}
                    }
                }
              }
            }
        );

        let agg_req: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        let res = exec_request(agg_req, &index)?;
        println!("{}", serde_json::to_string_pretty(&res).unwrap());
        let expected_res = json!({
          "sales_over_time" : {
          "buckets" : [
            {
              "key_as_string" : "2015-01-01T00:00:00Z",
              "key" : 1420070400000000.0,
              "doc_count" : 4,
              "texts": {
                  "buckets": [
                    {
                      "doc_count": 2,
                      "key": "bbb"
                    },
                    {
                      "doc_count": 1,
                      "key": "ccc"
                    },
                    {
                      "doc_count": 1,
                      "key": "aaa"
                    }
                  ],
                  "doc_count_error_upper_bound": 0,
                  "sum_other_doc_count": 0
                }
            }
          ]
        }
        });
        assert_eq!(res, expected_res);

        // 1day
        let elasticsearch_compatible_json = json!(
            {
              "sales_over_time": {
                "date_histogram": {
                  "field": "date",
                  "fixed_interval": "1d"
                }
              }
            }
        );

        let agg_req: Aggregations =
            serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
                .unwrap();
        let res = exec_request(agg_req, &index)?;
        let expected_res = json!( {
          "sales_over_time": {
            "buckets": [
              {
                "doc_count": 2,
                "key": 1420070400000000.0,
                "key_as_string": "2015-01-01T00:00:00Z"
              },
              {
                "doc_count": 1,
                "key": 1420156800000000.0,
                "key_as_string": "2015-01-02T00:00:00Z"
              },
              {
                "doc_count": 0,
                "key": 1420243200000000.0,
                "key_as_string": "2015-01-03T00:00:00Z"
              },
              {
                "doc_count": 0,
                "key": 1420329600000000.0,
                "key_as_string": "2015-01-04T00:00:00Z"
              },
              {
                "doc_count": 0,
                "key": 1420416000000000.0,
                "key_as_string": "2015-01-05T00:00:00Z"
              },
              {
                "doc_count": 1,
                "key": 1420502400000000.0,
                "key_as_string": "2015-01-06T00:00:00Z"
              }
            ]
          }
        });
        assert_eq!(res, expected_res);

        Ok(())
    }
}
