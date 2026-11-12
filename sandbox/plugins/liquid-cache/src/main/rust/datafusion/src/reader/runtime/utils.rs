use std::collections::VecDeque;

use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{RowSelection, RowSelector},
    },
    schema::types::SchemaDescriptor,
};

pub(crate) fn get_root_column_ids(
    schema: &SchemaDescriptor,
    projection: &ProjectionMask,
) -> Vec<usize> {
    let mut root_mask = vec![false; schema.root_schema().get_fields().len()];

    for leaf_idx in 0..schema.num_columns() {
        if projection.leaf_included(leaf_idx) {
            let root_idx = schema.get_column_root_idx(leaf_idx);
            root_mask[root_idx] = true;
        }
    }

    root_mask
        .into_iter()
        .enumerate()
        .filter_map(|(idx, included)| included.then_some(idx))
        .collect()
}

pub(crate) fn offset_row_selection(selection: RowSelection, offset: usize) -> RowSelection {
    if offset == 0 {
        return selection;
    }

    let mut selected_count = 0;
    let mut skipped_count = 0;

    let mut selectors: Vec<RowSelector> = selection.into();

    let find = selectors.iter().position(|selector| match selector.skip {
        true => {
            skipped_count += selector.row_count;
            false
        }
        false => {
            selected_count += selector.row_count;
            selected_count > offset
        }
    });

    let split_idx = match find {
        Some(idx) => idx,
        None => {
            selectors.clear();
            return RowSelection::from(selectors);
        }
    };

    let mut new_selectors = Vec::with_capacity(selectors.len() - split_idx + 1);
    new_selectors.push(RowSelector::skip(skipped_count + offset));
    new_selectors.push(RowSelector::select(selected_count - offset));
    new_selectors.extend_from_slice(&selectors[split_idx + 1..]);

    RowSelection::from(new_selectors)
}

pub(crate) fn limit_row_selection(selection: RowSelection, mut limit: usize) -> RowSelection {
    let mut selectors: Vec<RowSelector> = selection.into();

    if limit == 0 {
        selectors.clear();
    }

    for (idx, selection) in selectors.iter_mut().enumerate() {
        if !selection.skip {
            if selection.row_count >= limit {
                selection.row_count = limit;
                selectors.truncate(idx + 1);
                break;
            } else {
                limit -= selection.row_count;
            }
        }
    }
    RowSelection::from(selectors)
}

/// Take the next batch from the selection queue.
/// The returning selection will have exactly the batch size, or less if the selection is exhausted.
pub(super) fn take_next_batch(
    selection: &mut VecDeque<RowSelector>,
    batch_size: usize,
) -> Option<Vec<RowSelector>> {
    let mut current_selected = 0;
    let mut rt = Vec::new();
    while let Some(mut front) = selection.pop_front() {
        if front.row_count + current_selected > batch_size {
            let to_select = batch_size - current_selected;
            if to_select > 0 {
                let mut sub_front = front;
                sub_front.row_count = to_select;
                rt.push(sub_front);
            }
            let remaining = front.row_count - to_select;
            front.row_count = remaining;
            selection.push_front(front);
            current_selected += to_select;
            break;
        } else {
            rt.push(front);
            current_selected += front.row_count;
        }
    }
    if current_selected == 0 {
        return None;
    }
    Some(rt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_next_batch() {
        {
            let mut queue = VecDeque::new();
            let selection = take_next_batch(&mut queue, 8);
            assert!(selection.is_none());
            assert!(queue.is_empty());
        }

        {
            let mut queue = VecDeque::from(vec![RowSelector::select(8)]);
            let selection = take_next_batch(&mut queue, 8).unwrap();
            assert_eq!(selection, vec![RowSelector::select(8)]);
            assert!(queue.is_empty());
        }

        {
            let mut queue = VecDeque::from(vec![RowSelector::select(10)]);
            let selection = take_next_batch(&mut queue, 8).unwrap();
            assert_eq!(selection, vec![RowSelector::select(8)]);
            assert_eq!(queue, vec![RowSelector::select(2)]);
        }

        {
            let mut queue = VecDeque::from(vec![
                RowSelector::select(2),
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(2),
                RowSelector::select(2),
            ]);
            let selection = take_next_batch(&mut queue, 8).unwrap();
            assert_eq!(
                selection,
                vec![
                    RowSelector::select(2),
                    RowSelector::skip(2),
                    RowSelector::select(2),
                    RowSelector::skip(2),
                ]
            );
            assert_eq!(queue, vec![RowSelector::select(2)]);
        }

        {
            let mut queue = VecDeque::from(vec![RowSelector::select(3), RowSelector::skip(2)]);
            let selection = take_next_batch(&mut queue, 8).unwrap();
            assert_eq!(
                selection,
                vec![RowSelector::select(3), RowSelector::skip(2)]
            );
            assert!(queue.is_empty());
        }

        {
            let mut queue = VecDeque::from(vec![
                RowSelector::select(2),
                RowSelector::skip(4),
                RowSelector::select(6),
            ]);
            let selection = take_next_batch(&mut queue, 8).unwrap();
            assert_eq!(
                selection,
                vec![
                    RowSelector::select(2),
                    RowSelector::skip(4),
                    RowSelector::select(2),
                ]
            );
            assert_eq!(queue, vec![RowSelector::select(4)]);
        }

        {
            let mut queue = VecDeque::from(vec![
                RowSelector::skip(5),
                RowSelector::select(3),
                RowSelector::skip(2),
                RowSelector::select(7),
            ]);
            let selection = take_next_batch(&mut queue, 8).unwrap();
            assert_eq!(
                selection,
                vec![RowSelector::skip(5), RowSelector::select(3),]
            );
            assert_eq!(queue, vec![RowSelector::skip(2), RowSelector::select(7)]);
        }
    }
}
