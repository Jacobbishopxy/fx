//! file: fx_sizing_dqs_test.rs
//! author: Jacob Xie
//! date: 2023/05/07 11:43:25 Sunday
//! brief:

#[allow(dead_code)]
fn problem() {
    let _vec = vec![
        vec![vec![1, 2], vec![3, 4, 5], vec![6]],
        vec![vec![7, 8, 9], vec![10], vec![11, 12]],
        vec![vec![13, 14], vec![15, 16, 17, 18]],
    ];
    // the inner vec len
    let _simplified_vec = vec![
        //
        vec![2, 3, 1],
        vec![3, 1, 2],
        vec![2, 4],
    ];

    // to transform into a vector as below, and the whole process requires the minimum memory allocation:
    let _res = vec![
        vec![vec![1, 2, 3], vec![4], vec![5, 6]],
        vec![vec![7, 8, 9], vec![10], vec![11, 12]],
        vec![vec![13, 14, 15], vec![16], vec![17, 17]],
    ];
    // the inner vec len
    let _simplified_res = vec![
        //
        vec![3, 1, 2],
        vec![3, 1, 2],
        vec![3, 1, 2],
    ];

    // or this (worse than the case below):
    let _res = vec![
        vec![vec![1, 2, 3, 4], vec![5, 6]],
        vec![vec![7, 8, 9, 10], vec![11, 12]],
        vec![vec![13, 14, 15, 16], vec![17, 17]],
    ];
    let _simplified_res = vec![
        //
        vec![4, 2],
        vec![4, 2],
        vec![4, 2],
    ];
}

fn take_lengths(v: &[Vec<Vec<i32>>]) -> Vec<Vec<usize>> {
    let mut res = Vec::<Vec<usize>>::new();
    for (idx, i) in v.iter().enumerate() {
        for j in i.iter() {
            let j_len = j.len();
            match res.get(idx) {
                Some(_) => res[idx].push(j_len),
                None => res.push(vec![j_len]),
            }
        }
    }

    res
}

fn take_lengths_avg(v: &[Vec<Vec<i32>>]) -> Vec<usize> {
    let mut res = take_lengths(&v);

    res.iter_mut()
        .map(|r| r.iter().sum::<usize>() / r.len())
        .collect()
}

#[test]
fn test_transform() {
    let input = vec![
        vec![vec![1, 2, 3], vec![4, 5], vec![6]],
        vec![vec![7, 8, 9], vec![10, 11], vec![12]],
        vec![vec![13, 14, 15], vec![16, 17, 18]],
    ];

    println!("{:?}", take_lengths_avg(&input));

    let input = vec![
        vec![vec![1, 2], vec![3, 4, 5], vec![6]],
        vec![vec![7, 8, 9], vec![10], vec![11, 12]],
        vec![vec![13, 14], vec![15, 16, 17, 18]],
    ];

    println!("{:?}", take_lengths_avg(&input));
}
