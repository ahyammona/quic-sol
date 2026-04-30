use quic_sol_shared::QuicSolError;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Pair {
    SolUsdc,
    BtcUsdc,
    EthUsdc,
}

impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Pair::SolUsdc => write!(f, "SOL/USDC"),
            Pair::BtcUsdc => write!(f, "BTC/USDC"),
            Pair::EthUsdc => write!(f, "ETH/USDC"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub pair: String,
    pub price: f64,
    pub confidence: f64,
}

pub struct PriceFeedBuilder {
    endpoint: Option<String>,
    pairs: Vec<Pair>,
}

impl PriceFeedBuilder {
    pub fn new() -> Self {
        PriceFeedBuilder {
            endpoint: None,
            pairs: vec![],
        }
    }

    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    pub fn pairs(mut self, pairs: Vec<Pair>) -> Self {
        self.pairs = pairs;
        self
    }

    pub async fn connect(self) -> Result<PriceFeed, QuicSolError> {
        let (sender, receiver) = mpsc::channel(100);
        
        tokio::spawn(async move {
            // Mock ticks
            let ticks = vec![
                Tick {
                    pair: "SOL/USDC".to_string(),
                    price: 150.0,
                    confidence: 0.01,
                },
                Tick {
                    pair: "BTC/USDC".to_string(),
                    price: 60000.0,
                    confidence: 10.0,
                },
            ];
            for tick in ticks {
                if sender.send(tick).await.is_err() {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });
        
        Ok(PriceFeed { receiver })
    }
}

pub struct PriceFeed {
    receiver: mpsc::Receiver<Tick>,
}

impl PriceFeed {
    pub fn builder() -> PriceFeedBuilder {
        PriceFeedBuilder::new()
    }

    pub async fn next(&mut self) -> Result<Option<Tick>, QuicSolError> {
        Ok(self.receiver.recv().await)
    }
}