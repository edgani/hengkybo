# IDX Flow Engine V4 Architecture

V4 is designed as a probability engine, not a mind-reading tool.

Core layers:
1. Daily price/volume regime and phase features
2. Broker inventory and dynamic broker profile features
3. Intraday tape + order book confirmation
4. Rule scores for accumulation, breakout integrity, distribution risk, and microstructure strength
5. Walk-forward ranking model + calibration + confidence
6. Explainability and verdict mapping

The package is demo-runnable using synthetic-but-structured CSV data so the full pipeline can be validated locally.
