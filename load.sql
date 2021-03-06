select * from ( 
	select
		to_char(to_timestamp(receipt_included_in_block_timestamp / 1000000000), 'yyyy-MM-dd"T"HH24:MI:SS.US"Z"') as ts,
		receipt_id,
		index_in_action_receipt,
		action_kind,
		args->>'deposit' as deposit,
		args->>'method_name' as method_name,
		args->>'args_json' as args_json,
		args->'args_json'->>'receiver_id' as args_receiver_id,
		receipt_receiver_account_id,
		receipt_predecessor_account_id,
		transaction_hash,
		signer_account_id,
		signer_public_key
	from action_receipt_actions
	join receipts using (receipt_id)
	join transactions on originated_from_transaction_hash = transaction_hash
	order by receipt_included_in_block_timestamp
	limit 1000
) filtered
order by ts, index_in_action_receipt