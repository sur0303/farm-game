# CAP-REDIS-L1-R2 report

- Verdict: **PASS**
- Classification: exact R2 Product Redis/serialization qualification
- `baseline_eligible=false`
- `supports_30m_dau=false`
- Warmup/formal rounds: `1/5`
- Profile hash: `46592a3874444f0fd92a2f725b7d3830091c4bd690f7001d9a12780a98150bd8`

## Fault qualification

- Verdict: `PASS` (test head `fd99df569e42232cce2eb63323c9d09a2cb41b8a`)
- NOSCRIPT exact-source reload: `PASS`
- Connection reconstruction after server-side kill: `PASS`
- Post-write response loss: `TIMEOUT_UNKNOWN` resolved `COMMITTED` from the durable receipt
- Fault Redis instances were isolated and exactly destroyed; no `SCRIPT FLUSH` was used.

## Primitive matrix

| Operation | Key distribution | C | Redis commands/op | Req bytes/op | Resp bytes/op | Client P99 us | Lua P99 us | Cross-round commands CV |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 1 | 4.000 | 1729.0 | 1642.5 | 755.5 | 138.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 10 | 4.000 | 1729.0 | 1642.5 | 2113.0 | 167.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 100 | 4.000 | 1729.0 | 1642.5 | 17063.5 | 132.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | hot_slot_distinct_actor_keys | 300 | 4.000 | 1729.0 | 1642.5 | 58545.3 | 138.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 1 | 4.000 | 1734.0 | 1642.5 | 748.7 | 139.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 10 | 4.000 | 1734.0 | 1642.5 | 2209.4 | 168.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 100 | 4.000 | 1734.1 | 1642.5 | 16559.0 | 129.0 | 0.0000 |
| actor_timeout_unknown_fence_resolution | uniform_key | 300 | 4.000 | 1734.1 | 1642.5 | 52499.4 | 129.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 1 | 1.000 | 3790.6 | 51.5 | 503.4 | 220.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 10 | 1.000 | 3790.6 | 51.5 | 2136.7 | 226.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 100 | 1.000 | 3790.6 | 51.5 | 24833.6 | 221.0 | 0.0000 |
| farm_actor_commit_order_due | hot_slot_distinct_actor_keys | 300 | 1.000 | 3790.6 | 51.5 | 73652.3 | 221.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 1 | 1.000 | 3797.4 | 51.5 | 588.7 | 285.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 10 | 1.000 | 3797.4 | 51.5 | 2125.0 | 236.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 100 | 1.000 | 3797.1 | 51.5 | 27057.5 | 234.0 | 0.0000 |
| farm_actor_commit_order_due | uniform_key | 300 | 1.000 | 3797.1 | 51.5 | 76721.6 | 251.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 1 | 1.000 | 2543.5 | 51.5 | 406.9 | 167.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 10 | 1.000 | 2543.5 | 51.5 | 1713.5 | 156.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 100 | 1.000 | 2543.5 | 51.5 | 20847.4 | 172.0 | 0.0000 |
| farm_actor_commit_receipt | hot_slot_distinct_actor_keys | 300 | 1.000 | 2543.5 | 51.5 | 61316.4 | 163.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 1 | 1.000 | 2549.6 | 51.5 | 450.6 | 185.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 10 | 1.000 | 2549.4 | 51.5 | 1962.1 | 182.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 100 | 1.000 | 2549.2 | 51.5 | 19827.6 | 173.0 | 0.0000 |
| farm_actor_commit_receipt | uniform_key | 300 | 1.000 | 2549.4 | 51.5 | 64921.4 | 165.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 1 | 3.000 | 1849.0 | 1163.5 | 599.8 | 146.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 10 | 3.000 | 1849.0 | 1163.5 | 1902.3 | 151.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 100 | 3.000 | 1849.0 | 1163.5 | 15491.1 | 131.0 | 0.0000 |
| farm_actor_load_and_claim | hot_slot_distinct_actor_keys | 300 | 3.000 | 1849.0 | 1163.5 | 44993.7 | 142.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 1 | 3.000 | 1853.3 | 1163.5 | 567.5 | 151.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 10 | 3.000 | 1853.1 | 1163.5 | 1971.0 | 157.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 100 | 3.000 | 1853.2 | 1163.5 | 16528.0 | 144.0 | 0.0000 |
| farm_actor_load_and_claim | uniform_key | 300 | 3.000 | 1853.3 | 1163.5 | 47856.8 | 142.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 1 | 6.000 | 1669.0 | 2616.0 | 590.2 | 0.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 10 | 6.000 | 1669.0 | 2616.0 | 1433.7 | 0.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 100 | 6.000 | 1669.0 | 2616.0 | 11036.2 | 0.0 | 0.0000 |
| friend_index_scan_20 | hot_friend_index_key_20_entries | 300 | 6.000 | 1669.0 | 2616.0 | 28777.4 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 1 | 6.000 | 1669.0 | 2616.0 | 576.0 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 10 | 6.000 | 1669.0 | 2616.0 | 1421.6 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 100 | 6.000 | 1669.0 | 2616.0 | 11946.8 | 0.0 | 0.0000 |
| friend_index_scan_20 | uniform_friend_index_keys | 300 | 6.000 | 1669.0 | 2616.0 | 32729.2 | 0.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 1 | 7.000 | 4603.0 | 2754.0 | 1519.1 | 198.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 10 | 7.000 | 4591.0 | 2752.0 | 4127.4 | 198.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 100 | 7.000 | 4603.0 | 2754.0 | 34178.5 | 207.0 | 0.0000 |
| friend_pair_activate | hot_pair_key_idempotent_replay | 300 | 7.000 | 4609.0 | 2754.0 | 92461.9 | 166.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 1 | 5.000 | 7131.4 | 1102.2 | 2534.7 | 972.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 10 | 5.000 | 7131.3 | 1101.6 | 11904.1 | 932.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 100 | 5.000 | 7131.3 | 1093.4 | 134853.8 | 1003.0 | 0.0000 |
| friend_pair_activate | uniform_pair_keys | 300 | 5.000 | 7132.0 | 1073.6 | 406349.2 | 1002.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 1 | 18.000 | 73198.0 | 16258.0 | 9270.4 | 303.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 10 | 19.000 | 77126.0 | 16488.0 | 40994.0 | 216.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 100 | 16.000 | 65359.0 | 15799.0 | 387220.1 | 238.0 | 0.0000 |
| friend_pair_batch_read_20 | hot_20_pair_keys_and_bucket_envelopes | 300 | 16.000 | 65346.0 | 15797.0 | 1239935.9 | 251.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 1 | 20.000 | 81048.0 | 16718.0 | 9689.7 | 207.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 10 | 16.600 | 67707.9 | 15963.2 | 41591.8 | 303.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 100 | 17.190 | 70021.3 | 16204.7 | 423579.5 | 264.0 | 0.0000 |
| friend_pair_batch_read_20 | uniform_viewer_rosters | 300 | 17.273 | 70350.0 | 16247.4 | 1295270.4 | 266.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 1 | 1.000 | 803.0 | 4.0 | 276.0 | 96.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 10 | 1.000 | 803.0 | 4.0 | 1016.1 | 92.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 100 | 1.000 | 803.0 | 4.0 | 9576.0 | 91.0 | 0.0000 |
| friend_projection_repair | hot_viewer_projection_key_distinct_friend_fields | 300 | 1.000 | 803.0 | 4.0 | 32402.3 | 79.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 1 | 1.000 | 803.0 | 4.0 | 243.5 | 76.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 10 | 1.000 | 803.0 | 4.0 | 727.7 | 66.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 100 | 1.000 | 803.0 | 4.0 | 6493.1 | 61.0 | 0.0000 |
| friend_projection_repair | uniform_viewer_projection_keys | 300 | 1.000 | 803.0 | 4.0 | 23284.6 | 69.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 1 | 12.000 | 4543.0 | 3081.0 | 2020.9 | 109.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 10 | 12.000 | 4537.0 | 3081.0 | 4316.8 | 122.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 100 | 12.000 | 4543.0 | 3081.0 | 33867.4 | 110.0 | 0.0000 |
| friend_quota_reserve | hot_quota_key_idempotent_replay | 300 | 12.000 | 4526.0 | 3079.0 | 95932.0 | 98.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 1 | 6.000 | 5333.8 | 739.0 | 2172.5 | 837.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 10 | 6.000 | 5333.0 | 737.9 | 10988.4 | 816.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 100 | 6.000 | 5333.6 | 732.0 | 103132.3 | 731.0 | 0.0000 |
| friend_quota_reserve | uniform_quota_keys | 300 | 6.000 | 5333.3 | 709.8 | 292539.5 | 729.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 1 | 12.000 | 4126.0 | 3092.0 | 2054.9 | 107.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 10 | 12.000 | 4119.0 | 3092.0 | 4391.4 | 124.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 100 | 12.000 | 4126.0 | 3092.0 | 32795.6 | 109.0 | 0.0000 |
| friend_share_generate | hot_share_issuer_key_idempotent_replay | 300 | 12.000 | 4119.0 | 3092.0 | 95415.5 | 88.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 1 | 5.000 | 5117.5 | 835.3 | 2142.4 | 867.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 10 | 5.000 | 5117.5 | 834.7 | 9319.3 | 790.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 100 | 5.000 | 5117.6 | 827.2 | 102623.4 | 746.0 | 0.0000 |
| friend_share_generate | uniform_share_issuer_keys | 300 | 5.000 | 5117.6 | 803.8 | 277160.9 | 747.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 1 | 1.000 | 2564.5 | 51.5 | 440.6 | 176.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 10 | 1.000 | 2564.5 | 51.5 | 1854.8 | 161.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 100 | 1.000 | 2564.5 | 51.5 | 21031.6 | 157.0 | 0.0000 |
| player_actor_commit_receipt | hot_slot_distinct_actor_keys | 300 | 1.000 | 2564.5 | 51.5 | 62527.4 | 171.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 1 | 1.000 | 2570.5 | 51.5 | 426.6 | 177.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 10 | 1.000 | 2570.3 | 51.5 | 1688.6 | 162.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 100 | 1.000 | 2570.3 | 51.5 | 20580.3 | 176.0 | 0.0000 |
| player_actor_commit_receipt | uniform_key | 300 | 1.000 | 2570.5 | 51.5 | 66340.6 | 184.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 1 | 3.000 | 1863.7 | 1162.4 | 585.8 | 148.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 10 | 3.000 | 1864.0 | 1162.5 | 1989.2 | 158.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 100 | 3.000 | 1865.2 | 1163.1 | 15536.5 | 138.0 | 0.0000 |
| player_actor_load_and_claim | hot_slot_distinct_actor_keys | 300 | 3.000 | 1866.0 | 1163.5 | 48269.0 | 135.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 1 | 3.000 | 1864.3 | 1160.6 | 592.7 | 181.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 10 | 3.045 | 1869.6 | 1163.9 | 2037.0 | 155.0 | 0.0000 |
| player_actor_load_and_claim | uniform_key | 100 | 3.450 | 1901.2 | 1185.2 | 18330.0 | 145.0 | 0.0006 |
| player_actor_load_and_claim | uniform_key | 300 | 4.000 | 1944.2 | 1214.1 | 69126.3 | 143.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 1 | 1.000 | 1007.5 | 19.0 | 251.5 | 57.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 10 | 1.000 | 1007.5 | 19.0 | 661.7 | 57.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 100 | 1.000 | 1007.5 | 19.0 | 5386.6 | 47.0 | 0.0000 |
| result_directory_publish | hot_player_directory_key_distinct_fields | 300 | 1.000 | 1007.5 | 19.0 | 16295.6 | 47.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 1 | 1.000 | 1007.5 | 19.0 | 278.5 | 57.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 10 | 1.000 | 1007.5 | 19.0 | 625.5 | 54.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 100 | 1.000 | 1007.5 | 19.0 | 4848.0 | 46.0 | 0.0000 |
| result_directory_publish | uniform_player_directory_keys | 300 | 1.000 | 1007.5 | 19.0 | 15414.0 | 45.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 1 | 4.000 | 5416.0 | 114.0 | 416.4 | 49.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 10 | 4.000 | 5416.0 | 114.0 | 1440.3 | 47.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 100 | 4.000 | 5416.0 | 114.0 | 16885.2 | 41.0 | 0.0000 |
| session_route_renew_partial_r4 | hot_4_session_route_keys | 300 | 4.000 | 5416.0 | 114.0 | 50152.1 | 48.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 1 | 4.000 | 5416.0 | 114.0 | 413.0 | 55.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 10 | 4.000 | 5416.0 | 114.0 | 1378.9 | 48.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 100 | 4.000 | 5416.0 | 114.0 | 17658.4 | 48.0 | 0.0000 |
| session_route_renew_partial_r4 | uniform_session_route_keys | 300 | 4.000 | 5416.0 | 114.0 | 52605.7 | 51.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 1 | 1.000 | 1354.0 | 41.0 | 235.4 | 67.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 10 | 1.000 | 1354.0 | 41.0 | 963.5 | 62.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 100 | 1.000 | 1354.0 | 41.0 | 7437.8 | 52.0 | 0.0000 |
| session_route_renew_r1 | hot_1_session_route_keys | 300 | 1.000 | 1354.0 | 41.0 | 23223.9 | 56.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 1 | 1.000 | 1354.0 | 41.0 | 245.7 | 65.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 10 | 1.000 | 1354.0 | 41.0 | 802.7 | 68.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 100 | 1.000 | 1354.0 | 41.0 | 7730.8 | 62.0 | 0.0000 |
| session_route_renew_r1 | uniform_session_route_keys | 300 | 1.000 | 1354.0 | 41.0 | 24018.6 | 69.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 1 | 4.000 | 5416.0 | 164.0 | 411.8 | 61.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 10 | 4.000 | 5416.0 | 164.0 | 1656.9 | 50.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 100 | 4.000 | 5416.0 | 164.0 | 20877.6 | 51.0 | 0.0000 |
| session_route_renew_r4 | hot_4_session_route_keys | 300 | 4.000 | 5416.0 | 164.0 | 65842.5 | 52.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 1 | 4.000 | 5416.0 | 164.0 | 415.7 | 59.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 10 | 4.000 | 5416.0 | 164.0 | 1707.3 | 52.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 100 | 4.000 | 5416.0 | 164.0 | 21166.0 | 50.0 | 0.0000 |
| session_route_renew_r4 | uniform_session_route_keys | 300 | 4.000 | 5416.0 | 164.0 | 73966.2 | 49.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 1 | 1.000 | 1351.0 | 16.0 | 218.9 | 50.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 10 | 1.000 | 1351.0 | 16.0 | 594.0 | 47.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 100 | 1.000 | 1351.0 | 16.0 | 4973.3 | 37.0 | 0.0000 |
| session_route_renew_stale | hot_1_session_route_keys | 300 | 1.000 | 1351.0 | 16.0 | 16042.5 | 40.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 1 | 1.000 | 1351.0 | 16.0 | 212.1 | 48.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 10 | 1.000 | 1351.0 | 16.0 | 624.6 | 52.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 100 | 1.000 | 1351.0 | 16.0 | 5004.8 | 38.0 | 0.0000 |
| session_route_renew_stale | uniform_session_route_keys | 300 | 1.000 | 1351.0 | 16.0 | 13987.8 | 39.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 1 | 1.000 | 260.0 | 168.0 | 261.6 | 70.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 10 | 1.000 | 260.0 | 168.0 | 817.7 | 61.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 100 | 1.000 | 260.0 | 168.0 | 6806.5 | 58.0 | 0.0000 |
| shard_authority_renew | hot_shard_lease_key | 300 | 1.000 | 259.0 | 167.0 | 22011.6 | 54.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 1 | 1.000 | 260.0 | 168.0 | 264.5 | 64.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 10 | 1.000 | 258.0 | 166.0 | 693.7 | 64.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 100 | 1.000 | 257.9 | 165.9 | 7304.4 | 56.0 | 0.0000 |
| shard_authority_renew | uniform_shard_lease_keys | 300 | 1.000 | 257.8 | 165.8 | 21518.1 | 66.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 1 | 0.000 | 0.0 | 0.0 | 10.8 | 0.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 10 | 0.000 | 0.0 | 0.0 | 22.6 | 0.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 100 | 0.000 | 0.0 | 0.0 | 25.3 | 0.0 | 0.0000 |
| subscription_renew_r1_serialization | no_redis_key_protocol_serialization | 300 | 0.000 | 0.0 | 0.0 | 34.3 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 1 | 0.000 | 0.0 | 0.0 | 20.4 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 10 | 0.000 | 0.0 | 0.0 | 27.8 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 100 | 0.000 | 0.0 | 0.0 | 34.6 | 0.0 | 0.0000 |
| subscription_renew_r4_serialization | no_redis_key_protocol_serialization | 300 | 0.000 | 0.0 | 0.0 | 40.1 | 0.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 1 | 1.000 | 4621.7 | 12.0 | 561.2 | 96.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 10 | 1.000 | 4621.7 | 12.0 | 1255.5 | 103.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 100 | 1.000 | 4621.7 | 12.0 | 10122.8 | 108.0 | 0.0000 |
| workflow_advance | hot_due_index_distinct_record_keys | 300 | 1.000 | 4621.7 | 12.0 | 32652.3 | 100.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 1 | 1.000 | 4618.7 | 12.0 | 530.0 | 102.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 10 | 1.000 | 4619.0 | 12.0 | 1223.6 | 102.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 100 | 1.000 | 4621.7 | 12.0 | 10651.7 | 98.0 | 0.0000 |
| workflow_advance | uniform_record_keys_shared_due_index | 300 | 1.000 | 4621.7 | 12.0 | 33840.8 | 91.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 1 | 1.000 | 1516.7 | 2590.7 | 703.0 | 96.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 10 | 1.000 | 1516.7 | 2590.7 | 1474.5 | 118.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 100 | 1.000 | 1516.7 | 2590.7 | 8379.9 | 98.0 | 0.0000 |
| workflow_claim | hot_due_index_distinct_record_keys | 300 | 1.000 | 1516.7 | 2590.7 | 29742.4 | 93.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 1 | 1.000 | 1516.7 | 2590.7 | 695.7 | 100.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 10 | 1.000 | 1516.7 | 2590.7 | 1541.8 | 113.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 100 | 1.000 | 1516.7 | 2590.7 | 9135.1 | 83.0 | 0.0000 |
| workflow_claim | uniform_record_keys_shared_due_index | 300 | 1.000 | 1516.7 | 2590.7 | 32171.5 | 97.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 1 | 1.000 | 3457.7 | 8.0 | 543.5 | 88.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 10 | 1.000 | 3457.7 | 8.0 | 1038.0 | 97.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 100 | 1.000 | 3457.7 | 8.0 | 7222.0 | 84.0 | 0.0000 |
| workflow_create_if_absent | hot_due_index_distinct_record_keys | 300 | 1.000 | 3457.7 | 8.0 | 26066.6 | 79.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 1 | 1.000 | 3457.7 | 8.0 | 539.7 | 90.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 10 | 1.000 | 3457.7 | 8.0 | 1044.6 | 93.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 100 | 1.000 | 3457.7 | 8.0 | 7055.5 | 81.0 | 0.0000 |
| workflow_create_if_absent | uniform_record_keys_shared_due_index | 300 | 1.000 | 3457.7 | 8.0 | 27728.6 | 88.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 1 | 2.000 | 1718.7 | 2755.3 | 874.7 | 84.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 10 | 2.000 | 1718.7 | 2755.3 | 1618.6 | 104.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 100 | 2.000 | 1718.7 | 2755.3 | 11067.1 | 70.0 | 0.0000 |
| workflow_renew | hot_due_index_distinct_record_keys | 300 | 2.000 | 1718.7 | 2755.3 | 28715.0 | 69.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 1 | 2.000 | 1718.7 | 2755.3 | 839.4 | 77.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 10 | 2.000 | 1718.7 | 2755.3 | 1550.1 | 102.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 100 | 2.000 | 1718.7 | 2755.3 | 11505.0 | 82.0 | 0.0000 |
| workflow_renew | uniform_record_keys_shared_due_index | 300 | 2.000 | 1718.7 | 2755.3 | 28517.9 | 68.0 | 0.0000 |

## Business-command amplification

Measured through an operation-bound transparent RESP proxy around real H5 command journeys; background polling is excluded by in-memory operation/derived-work aliases.

| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | Total bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1 | 0 | 0 | 0 | 1 | 1 | 2497 |
| PLANT | 3 | 7 | 0 | 7 | 3 | 10 | 44259 |
| HARVEST | 2 | 5 | 0 | 5 | 2 | 7 | 25670 |
| SELL | 1 | 0 | 0 | 0 | 1 | 1 | 3832 |
| WATER | 1 | 0 | 1 | 1 | 1 | 7 | 19528 |
| THEFT | 2 | 5 | 0 | 5 | 2 | 39 | 68557 |
| CLAIM_TASK | 1 | 0 | 0 | 0 | 1 | 1 | 6077 |

Warmup results are retained but excluded from aggregate statistics. No Mixed B0 eligibility is granted by this report.
