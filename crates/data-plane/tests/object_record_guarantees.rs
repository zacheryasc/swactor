//! Black-box contract tests for reusable MO01 object-record framing.

use data_plane::object_record as object;

fn token_spec() -> object::ObjectSpec {
    object::ObjectSpec {
        max_extent: 16,
        alignment: 4,
        layout: object::ObjectLayout::Token,
    }
}

fn record(sequence: u64, extent: u64) -> Vec<u8> {
    object::ObjectRecordBuilder::new(token_spec())
        .object_id(object::ObjectId(9000 + sequence))
        .sequence(sequence)
        .extent(extent)
        .payload(vec![sequence as u8; extent as usize])
        .encode()
}

#[test]
fn header_validation_rejects_invalid_object_records() {
    let cases = [
        (
            object::ObjectRecordBuilder::new(token_spec())
                .unsupported_magic()
                .encode(),
            object::ObjectFailureReason::UnsupportedMagic,
        ),
        (
            object::ObjectRecordBuilder::new(token_spec())
                .unsupported_version()
                .encode(),
            object::ObjectFailureReason::UnsupportedVersion,
        ),
        (
            object::ObjectRecordBuilder::new(token_spec())
                .malformed_header_length()
                .encode(),
            object::ObjectFailureReason::MalformedHeaderLength,
        ),
        (
            object::ObjectRecordBuilder::new(token_spec())
                .extent(32)
                .payload(vec![0; 32])
                .encode(),
            object::ObjectFailureReason::ExtentExceedsMax,
        ),
        (
            object::ObjectRecordBuilder::new(token_spec())
                .extent(6)
                .payload(vec![0; 6])
                .encode(),
            object::ObjectFailureReason::ExtentAlignmentViolation,
        ),
    ];

    for (bytes, expected) in cases {
        assert_eq!(
            object::read_object_record(&bytes, token_spec(), true),
            Err(expected)
        );
    }
}

#[test]
fn flags_round_trip_through_public_header_contract() {
    let record = object::ObjectRecordBuilder::new(token_spec())
        .object_id(object::ObjectId(9000))
        .sequence(0)
        .extent(8)
        .payload(vec![0; 8])
        .flags(object::ObjectFlags {
            end_of_sequence: true,
            begin_sequence: true,
        })
        .encode();

    let parsed = object::read_object_record(&record, token_spec(), true).expect("record parses");
    let object::ObjectRecordRead::Complete(parsed) = parsed else {
        panic!("record must be complete");
    };
    assert_eq!(
        parsed.flags,
        object::ObjectFlags {
            end_of_sequence: true,
            begin_sequence: true,
        }
    );
}

#[test]
fn complete_records_split_by_total_len_and_partial_eof_faults() {
    let first = record(0, 8);
    let second = record(1, 4);
    let mut joined = first.clone();
    joined.extend_from_slice(&second);

    let parsed = object::read_object_record(&joined, token_spec(), false)
        .expect("joined stream starts with complete record");
    let object::ObjectRecordRead::Complete(first_record) = parsed else {
        panic!("first record must be complete");
    };
    assert_eq!(first_record.object_id, object::ObjectId(9000));
    assert_eq!(first_record.sequence, 0);
    assert_eq!(first_record.extent, 8);
    assert_eq!(first_record.total_len, first.len());

    let parsed_second =
        object::read_object_record(&joined[first_record.total_len..], token_spec(), true)
            .expect("second record starts at first total_len");
    let object::ObjectRecordRead::Complete(second_record) = parsed_second else {
        panic!("second record must be complete");
    };
    assert_eq!(second_record.object_id, object::ObjectId(9001));
    assert_eq!(second_record.sequence, 1);
    assert_eq!(second_record.extent, 4);
    assert_eq!(second_record.total_len, second.len());

    assert_eq!(
        object::read_object_record(&joined[..object::HEADER_LEN - 1], token_spec(), false),
        Ok(object::ObjectRecordRead::Incomplete)
    );
    assert_eq!(
        object::read_object_record(&joined[..object::HEADER_LEN - 1], token_spec(), true),
        Err(object::ObjectFailureReason::EofBeforeFullPayload)
    );
}
