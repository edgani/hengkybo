# Bidirectional Burst Engine (V4.7)

This module adds a directional tape/orderbook burst layer on top of the existing EOD + broker-flow engine.

## Core idea
A "gulung volume" event is not inherently bullish or bearish. It is a microstructure event:
- up-roll: aggressive buying sweeps the offer
- down-roll: aggressive selling sweeps the bid

The engine therefore evaluates both directions with the same sequence:
1. initiative burst exists?
2. did price progress efficiently?
3. did the move follow through?
4. was it absorbed / trapped?
5. is it likely continuation, climax, or capitulation when blended with EOD context?

## Intraday scores
- `gulungan_up_score`
- `gulungan_down_score`
- `effort_result_up`
- `effort_result_down`
- `post_up_followthrough_score`
- `post_down_followthrough_score`
- `absorption_after_up_score`
- `absorption_after_down_score`
- `bullish_burst_score_intraday`
- `bearish_burst_score_intraday`
- `bull_trap_score_intraday`
- `bear_trap_score_intraday`

## Contextual scores
- `climax_up_risk`
- `capitulation_down_risk`
- `bullish_burst_score`
- `bearish_burst_score`
- `bull_trap_score`
- `bear_trap_score`
- `dominant_burst_label_context`

## Event labels
- `UP_INITIATIVE_SWEEP`
- `UP_CONTINUATION_BURST`
- `UP_FALSE_BREAKOUT_RISK`
- `UP_CLIMAX_RISK`
- `DOWN_INITIATIVE_SWEEP`
- `DOWN_CONTINUATION_BREAK`
- `DOWN_CAPITULATION_RISK`

## Output files
- `data/features/intraday_features_v4.csv`
- `data/features/burst_events_v47.csv`
- `data/features/latest_watchlist_v4.csv`

## Note
These scores are inferential. They read matched flow and price response, not intent.
