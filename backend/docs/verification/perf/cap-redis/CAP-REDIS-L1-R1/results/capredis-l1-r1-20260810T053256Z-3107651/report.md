# CAP-REDIS-L1-R1 report

- Verdict: **PASS**
- Classification: current R1 Integration Redis/serialization baseline
- `baseline_eligible=false`
- `supports_30m_dau=false`
- Warmup/formal rounds: `1/5`
- Profile hash: `eb4ee6ee0a69b760423495eb42bf3778d0e69309ac55be6eb161df52efff7729`

## Fault qualification

- Verdict: `PASS` (test head `5fbbda4ec521c1ea7590f867dc2f7186b4ca307e`)
- NOSCRIPT exact-source reload: `PASS`
- Connection reconstruction after server-side kill: `PASS`
- Post-write response loss: `TIMEOUT_UNKNOWN` resolved `COMMITTED` from the durable receipt
- Fault Redis instances were isolated and exactly destroyed; no `SCRIPT FLUSH` was used.

## Primitive matrix

| Operation | Key distribution | C | Redis commands/op | Req bytes/op | Resp bytes/op | Client P99 us | Lua P99 us | Cross-round commands CV |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 1 | 4.000 | 1696.0 | 1635.5 | 678.7 | 130.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 10 | 4.000 | 1696.0 | 1635.5 | 2045.4 | 155.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 100 | 4.000 | 1696.0 | 1635.5 | 17116.0 | 126.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 300 | 4.000 | 1696.0 | 1635.5 | 56522.4 | 120.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 1 | 4.000 | 1701.1 | 1635.5 | 697.7 | 160.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 10 | 4.000 | 1701.1 | 1635.5 | 2215.1 | 163.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 100 | 4.000 | 1701.0 | 1635.5 | 16491.7 | 125.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 300 | 4.000 | 1701.1 | 1635.5 | 58084.7 | 130.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 1 | 1.000 | 3557.1 | 51.5 | 472.0 | 209.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 10 | 1.000 | 3557.1 | 51.5 | 1956.9 | 203.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 100 | 1.000 | 3557.1 | 51.5 | 22696.5 | 191.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 300 | 1.000 | 3557.1 | 51.5 | 70865.5 | 211.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 1 | 1.000 | 3563.7 | 51.5 | 597.7 | 253.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 10 | 1.000 | 3563.8 | 51.5 | 1982.2 | 209.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 100 | 1.000 | 3563.9 | 51.5 | 24333.0 | 221.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 300 | 1.000 | 3563.8 | 51.5 | 80895.3 | 252.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 1 | 1.000 | 2310.0 | 51.5 | 375.7 | 164.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 10 | 1.000 | 2310.0 | 51.5 | 1587.9 | 146.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 100 | 1.000 | 2310.0 | 51.5 | 17777.0 | 147.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 300 | 1.000 | 2310.0 | 51.5 | 62619.1 | 159.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 1 | 1.000 | 2315.9 | 51.5 | 370.9 | 152.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 10 | 1.000 | 2316.0 | 51.5 | 1727.5 | 154.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 100 | 1.000 | 2315.9 | 51.5 | 18853.6 | 141.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 300 | 1.000 | 2315.9 | 51.5 | 57337.8 | 153.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 1 | 3.000 | 1741.0 | 1156.5 | 623.3 | 157.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 10 | 3.000 | 1741.0 | 1156.5 | 1764.9 | 144.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 100 | 3.000 | 1741.0 | 1156.5 | 16119.0 | 143.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 300 | 3.000 | 1741.0 | 1156.5 | 52745.6 | 133.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 1 | 3.000 | 1745.1 | 1156.5 | 482.1 | 132.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 10 | 3.000 | 1745.1 | 1156.5 | 1901.7 | 146.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 100 | 3.000 | 1745.3 | 1156.5 | 15832.6 | 136.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 300 | 3.000 | 1745.2 | 1156.5 | 49237.0 | 132.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 1 | 6.000 | 1669.0 | 2616.0 | 551.6 | 0.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 10 | 6.000 | 1669.0 | 2616.0 | 1456.2 | 0.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 100 | 6.000 | 1669.0 | 2616.0 | 10994.8 | 0.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 300 | 6.000 | 1669.0 | 2616.0 | 30624.1 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 1 | 6.000 | 1669.0 | 2616.0 | 588.3 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 10 | 6.000 | 1669.0 | 2616.0 | 1526.9 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 100 | 6.000 | 1669.0 | 2616.0 | 12017.2 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 300 | 6.000 | 1669.0 | 2616.0 | 35029.3 | 0.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 1 | 9.000 | 4880.0 | 2708.0 | 1748.3 | 98.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 10 | 9.000 | 4880.0 | 2708.0 | 3592.2 | 101.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 100 | 9.000 | 4880.0 | 2708.0 | 30535.6 | 86.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 300 | 9.000 | 4886.0 | 2708.0 | 81920.6 | 73.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 1 | 5.000 | 5810.8 | 807.3 | 2398.7 | 937.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 10 | 5.000 | 5811.9 | 806.9 | 12878.3 | 977.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 100 | 5.000 | 5811.4 | 799.5 | 130719.5 | 1069.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 300 | 5.000 | 5811.7 | 778.8 | 400644.5 | 1032.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 1 | 60.000 | 83628.0 | 15776.0 | 2746.2 | 65.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 10 | 60.000 | 83620.0 | 15775.0 | 16048.8 | 65.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 100 | 60.000 | 83644.0 | 15790.0 | 133884.8 | 59.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 300 | 60.000 | 83620.0 | 15775.0 | 401886.7 | 67.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 1 | 60.000 | 83660.0 | 15780.0 | 2932.9 | 70.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 10 | 60.000 | 83636.8 | 15812.7 | 16011.5 | 68.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 100 | 60.000 | 83632.6 | 15935.4 | 149089.8 | 69.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 300 | 60.000 | 83635.2 | 15964.7 | 431218.3 | 70.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 1 | 1.000 | 803.0 | 4.0 | 273.8 | 99.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 10 | 1.000 | 803.0 | 4.0 | 1073.0 | 89.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 100 | 1.000 | 803.0 | 4.0 | 9920.1 | 85.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 300 | 1.000 | 803.0 | 4.0 | 32659.0 | 80.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 1 | 1.000 | 803.0 | 4.0 | 225.9 | 70.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 10 | 1.000 | 803.0 | 4.0 | 873.1 | 66.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 100 | 1.000 | 803.0 | 4.0 | 6373.7 | 69.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 300 | 1.000 | 803.0 | 4.0 | 21316.8 | 62.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 1 | 12.000 | 4523.0 | 3061.0 | 2053.5 | 95.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 10 | 12.000 | 4523.0 | 3061.0 | 4218.1 | 109.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 100 | 12.000 | 4523.0 | 3061.0 | 32932.6 | 98.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 300 | 12.000 | 4523.0 | 3061.0 | 89874.1 | 85.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 1 | 6.000 | 5319.7 | 720.2 | 2140.9 | 795.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 10 | 6.000 | 5318.8 | 719.2 | 10396.8 | 813.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 100 | 6.000 | 5319.9 | 713.3 | 98876.3 | 735.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 300 | 6.000 | 5319.4 | 691.7 | 306887.4 | 736.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 1 | 12.000 | 4095.0 | 3070.0 | 2150.0 | 92.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 10 | 12.000 | 4112.0 | 3072.0 | 4166.0 | 103.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 100 | 12.000 | 4105.0 | 3072.0 | 32532.7 | 102.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 300 | 12.000 | 4112.0 | 3072.0 | 91117.1 | 92.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 1 | 5.000 | 5103.4 | 816.4 | 2224.0 | 925.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 10 | 5.000 | 5102.9 | 815.5 | 9923.0 | 827.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 100 | 5.000 | 5103.5 | 809.3 | 101658.9 | 718.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 300 | 5.000 | 5103.7 | 784.4 | 302512.5 | 759.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 1 | 1.000 | 2326.0 | 51.5 | 388.9 | 160.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 10 | 1.000 | 2326.0 | 51.5 | 1659.2 | 143.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 100 | 1.000 | 2326.0 | 51.5 | 17930.3 | 140.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 300 | 1.000 | 2326.0 | 51.5 | 59428.9 | 135.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 1 | 1.000 | 2331.9 | 51.5 | 376.2 | 156.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 10 | 1.000 | 2331.9 | 51.5 | 1669.1 | 157.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 100 | 1.000 | 2332.0 | 51.5 | 19437.0 | 164.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 300 | 1.000 | 2331.7 | 51.5 | 60682.8 | 143.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 1 | 3.000 | 1750.8 | 1155.4 | 556.9 | 145.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 10 | 3.000 | 1751.0 | 1155.5 | 1784.8 | 146.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 100 | 3.000 | 1752.2 | 1156.1 | 15688.1 | 129.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 300 | 3.000 | 1753.0 | 1156.5 | 44135.9 | 126.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 1 | 3.000 | 1751.5 | 1153.6 | 544.2 | 144.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 10 | 3.045 | 1756.8 | 1156.9 | 2085.3 | 154.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 100 | 3.450 | 1788.3 | 1178.2 | 17428.2 | 119.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 300 | 4.000 | 1831.2 | 1207.1 | 60884.0 | 145.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 1 | 1.000 | 1007.5 | 19.0 | 264.3 | 50.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 10 | 1.000 | 1007.5 | 19.0 | 640.8 | 54.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 100 | 1.000 | 1007.5 | 19.0 | 4542.7 | 43.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 300 | 1.000 | 1007.5 | 19.0 | 16668.0 | 40.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 1 | 1.000 | 1007.5 | 19.0 | 235.6 | 54.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 10 | 1.000 | 1007.5 | 19.0 | 673.0 | 62.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 100 | 1.000 | 1007.5 | 19.0 | 4831.5 | 43.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 300 | 1.000 | 1007.5 | 19.0 | 14204.8 | 44.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 1 | 4.000 | 5416.0 | 114.0 | 409.6 | 47.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 10 | 4.000 | 5416.0 | 114.0 | 1383.9 | 46.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 100 | 4.000 | 5416.0 | 114.0 | 16578.6 | 41.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 300 | 4.000 | 5416.0 | 114.0 | 47093.0 | 46.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 1 | 4.000 | 5416.0 | 114.0 | 414.2 | 58.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 10 | 4.000 | 5416.0 | 114.0 | 1361.9 | 51.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 100 | 4.000 | 5416.0 | 114.0 | 18231.2 | 44.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 300 | 4.000 | 5416.0 | 114.0 | 56134.3 | 50.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 1 | 1.000 | 1354.0 | 41.0 | 256.0 | 72.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 10 | 1.000 | 1354.0 | 41.0 | 671.5 | 58.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 100 | 1.000 | 1354.0 | 41.0 | 7817.3 | 57.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 300 | 1.000 | 1354.0 | 41.0 | 24252.6 | 53.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 1 | 1.000 | 1354.0 | 41.0 | 247.2 | 67.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 10 | 1.000 | 1354.0 | 41.0 | 813.1 | 65.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 100 | 1.000 | 1354.0 | 41.0 | 7668.5 | 55.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 300 | 1.000 | 1354.0 | 41.0 | 25493.8 | 60.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 1 | 4.000 | 5416.0 | 164.0 | 423.4 | 55.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 10 | 4.000 | 5416.0 | 164.0 | 2066.7 | 54.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 100 | 4.000 | 5416.0 | 164.0 | 21414.5 | 47.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 300 | 4.000 | 5416.0 | 164.0 | 72621.9 | 49.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 1 | 4.000 | 5416.0 | 164.0 | 412.2 | 57.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 10 | 4.000 | 5416.0 | 164.0 | 1635.2 | 55.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 100 | 4.000 | 5416.0 | 164.0 | 23462.3 | 50.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 300 | 4.000 | 5416.0 | 164.0 | 72873.6 | 50.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 1 | 1.000 | 1351.0 | 16.0 | 218.9 | 43.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 10 | 1.000 | 1351.0 | 16.0 | 554.1 | 49.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 100 | 1.000 | 1351.0 | 16.0 | 4610.7 | 38.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 300 | 1.000 | 1351.0 | 16.0 | 13584.5 | 41.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 1 | 1.000 | 1351.0 | 16.0 | 217.7 | 45.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 10 | 1.000 | 1351.0 | 16.0 | 665.2 | 49.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 100 | 1.000 | 1351.0 | 16.0 | 4659.1 | 38.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 300 | 1.000 | 1351.0 | 16.0 | 17008.7 | 34.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 1 | 1.000 | 260.0 | 168.0 | 231.4 | 66.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 10 | 1.000 | 260.0 | 168.0 | 641.7 | 58.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 100 | 1.000 | 260.0 | 168.0 | 7215.1 | 58.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 300 | 1.000 | 260.0 | 168.0 | 23245.3 | 65.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 1 | 1.000 | 260.0 | 168.0 | 256.9 | 62.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 10 | 1.000 | 257.7 | 165.7 | 670.8 | 57.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 100 | 1.000 | 257.8 | 165.8 | 7849.6 | 61.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 300 | 1.000 | 257.9 | 165.8 | 22189.9 | 70.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 1 | 0.000 | 0.0 | 0.0 | 11.7 | 0.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 10 | 0.000 | 0.0 | 0.0 | 22.4 | 0.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 100 | 0.000 | 0.0 | 0.0 | 24.9 | 0.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 300 | 0.000 | 0.0 | 0.0 | 359.8 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 1 | 0.000 | 0.0 | 0.0 | 18.8 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 10 | 0.000 | 0.0 | 0.0 | 29.2 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 100 | 0.000 | 0.0 | 0.0 | 31.6 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 300 | 0.000 | 0.0 | 0.0 | 449.3 | 0.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 1 | 1.000 | 4618.7 | 12.0 | 551.7 | 102.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 10 | 1.000 | 4618.7 | 12.0 | 1161.3 | 93.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 100 | 1.000 | 4618.7 | 12.0 | 9697.3 | 92.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 300 | 1.000 | 4618.7 | 12.0 | 33470.7 | 92.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 1 | 1.000 | 4618.7 | 12.0 | 543.3 | 101.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 10 | 1.000 | 4618.7 | 12.0 | 1136.5 | 97.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 100 | 1.000 | 4618.7 | 12.0 | 9909.7 | 98.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 300 | 1.000 | 4618.7 | 12.0 | 30124.5 | 91.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 1 | 1.000 | 1514.7 | 2589.7 | 708.4 | 108.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 10 | 1.000 | 1514.7 | 2589.7 | 1386.5 | 110.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 100 | 1.000 | 1516.7 | 2590.7 | 9253.8 | 85.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 300 | 1.000 | 1516.7 | 2590.7 | 30579.1 | 108.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 1 | 1.000 | 1514.7 | 2589.7 | 699.0 | 100.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 10 | 1.000 | 1514.7 | 2589.7 | 1435.5 | 121.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 100 | 1.000 | 1514.7 | 2589.7 | 9303.7 | 91.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 300 | 1.000 | 1514.7 | 2589.7 | 33375.0 | 91.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 1 | 1.000 | 3457.7 | 8.0 | 516.8 | 81.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 10 | 1.000 | 3457.7 | 8.0 | 1090.9 | 97.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 100 | 1.000 | 3457.7 | 8.0 | 8192.4 | 77.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 300 | 1.000 | 3457.7 | 8.0 | 25014.9 | 86.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 1 | 1.000 | 3457.7 | 8.0 | 528.5 | 90.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 10 | 1.000 | 3457.7 | 8.0 | 1008.7 | 93.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 100 | 1.000 | 3457.7 | 8.0 | 7713.5 | 80.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 300 | 1.000 | 3457.7 | 8.0 | 26105.9 | 82.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 1 | 2.000 | 1718.7 | 2755.3 | 822.8 | 70.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 10 | 2.000 | 1718.7 | 2755.3 | 1637.6 | 98.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 100 | 2.000 | 1718.7 | 2755.3 | 10943.9 | 70.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 300 | 2.000 | 1718.7 | 2755.3 | 30677.4 | 64.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 1 | 2.000 | 1718.7 | 2755.3 | 869.1 | 78.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 10 | 2.000 | 1718.7 | 2755.3 | 1496.2 | 92.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 100 | 2.000 | 1718.7 | 2755.3 | 13445.3 | 70.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 300 | 2.000 | 1718.7 | 2755.3 | 28339.2 | 63.0 | 0.0000 |

## Business-command amplification

Measured through an operation-bound transparent RESP proxy around real H5 command journeys; background polling is excluded by in-memory operation/derived-work aliases.

| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | Total bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1 | 0 | 0 | 0 | 1 | 1 | 2246 |
| PLANT | 3 | 7 | 0 | 7 | 3 | 10 | 43542 |
| HARVEST | 2 | 5 | 0 | 5 | 2 | 7 | 25230 |
| SELL | 1 | 0 | 0 | 0 | 1 | 1 | 3580 |
| WATER | 1 | 0 | 1 | 1 | 1 | 7 | 19284 |
| THEFT | 2 | 5 | 0 | 5 | 2 | 17 | 50401 |
| CLAIM_TASK | 1 | 0 | 0 | 0 | 1 | 1 | 5825 |

Warmup results are retained but excluded from aggregate statistics. No Mixed B0 eligibility is granted by this report.
