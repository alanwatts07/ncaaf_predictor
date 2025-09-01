# src/models/vme_model_clean.py
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from typing import Tuple, Dict, List, Optional
import logging
import json
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VariationalTeamEmbedding(nn.Module):
    """
    Variational Autoencoder for learning team embeddings.
    
    This model learns compact representations (embeddings) of teams
    based on their opponent-adjusted performance metrics.
    """
    
    def __init__(self, input_dim: int, embedding_dim: int = 32, hidden_dims: List[int] = None):
        super(VariationalTeamEmbedding, self).__init__()
        
        if hidden_dims is None:
            hidden_dims = [64, 48]
        
        self.input_dim = input_dim
        self.embedding_dim = embedding_dim
        self.hidden_dims = hidden_dims
        
        # Encoder
        encoder_layers = []
        prev_dim = input_dim
        
        for hidden_dim in hidden_dims:
            encoder_layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.ReLU(),
                nn.Dropout(0.2)
            ])
            prev_dim = hidden_dim
        
        self.encoder = nn.Sequential(*encoder_layers)
        
        # Variational bottleneck
        self.mu_layer = nn.Linear(prev_dim, embedding_dim)
        self.logvar_layer = nn.Linear(prev_dim, embedding_dim)
        
        # Decoder
        decoder_layers = []
        prev_dim = embedding_dim
        
        for hidden_dim in reversed(hidden_dims):
            decoder_layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.ReLU(),
                nn.Dropout(0.2)
            ])
            prev_dim = hidden_dim
        
        decoder_layers.append(nn.Linear(prev_dim, input_dim))
        self.decoder = nn.Sequential(*decoder_layers)
    
    def encode(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """Encode input to latent parameters."""
        h = self.encoder(x)
        mu = self.mu_layer(h)
        logvar = self.logvar_layer(h)
        return mu, logvar
    
    def reparameterize(self, mu: torch.Tensor, logvar: torch.Tensor) -> torch.Tensor:
        """Reparameterization trick for sampling."""
        std = torch.exp(0.5 * logvar)
        eps = torch.randn_like(std)
        return mu + eps * std
    
    def decode(self, z: torch.Tensor) -> torch.Tensor:
        """Decode latent representation to output."""
        return self.decoder(z)
    
    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """Forward pass through the VAE."""
        mu, logvar = self.encode(x)
        z = self.reparameterize(mu, logvar)
        recon = self.decode(z)
        return recon, mu, logvar
    
    def get_embedding(self, x: torch.Tensor) -> torch.Tensor:
        """Get team embedding (use mean of latent distribution)."""
        mu, _ = self.encode(x)
        return mu

def vae_loss_function(recon_x, x, mu, logvar, beta=1.0):
    """
    VAE loss function combining reconstruction and KL divergence.
    
    Args:
        recon_x: Reconstructed input
        x: Original input
        mu: Mean of latent distribution
        logvar: Log variance of latent distribution
        beta: Weight for KL divergence (beta-VAE)
    """
    # Reconstruction loss (MSE)
    recon_loss = nn.MSELoss(reduction='sum')(recon_x, x)
    
    # KL divergence
    kld = -0.5 * torch.sum(1 + logvar - mu.pow(2) - logvar.exp())
    
    return recon_loss + beta * kld, recon_loss, kld

class TeamEmbeddingTrainer:
    """
    Trainer for the team embedding model.
    """
    
    def __init__(self, model: VariationalTeamEmbedding, device: str = 'cpu'):
        self.model = model.to(device)
        self.device = device
        self.history = {'loss': [], 'recon_loss': [], 'kld_loss': []}
        
    def train(self, 
              train_data: np.ndarray, 
              epochs: int = 100, 
              batch_size: int = 32, 
              learning_rate: float = 1e-3,
              beta: float = 1.0) -> Dict[str, List[float]]:
        """
        Train the team embedding model.
        """
        
        # Convert to PyTorch tensors
        train_tensor = torch.FloatTensor(train_data).to(self.device)
        train_dataset = TensorDataset(train_tensor)
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        
        # Optimizer
        optimizer = optim.Adam(self.model.parameters(), lr=learning_rate)
        
        # Training loop
        self.model.train()
        for epoch in range(epochs):
            epoch_loss = 0
            epoch_recon = 0
            epoch_kld = 0
            
            for batch_idx, (data,) in enumerate(train_loader):
                optimizer.zero_grad()
                
                # Forward pass
                recon_data, mu, logvar = self.model(data)
                
                # Calculate loss
                loss, recon_loss, kld = vae_loss_function(recon_data, data, mu, logvar, beta)
                
                # Backward pass
                loss.backward()
                optimizer.step()
                
                epoch_loss += loss.item()
                epoch_recon += recon_loss.item()
                epoch_kld += kld.item()
            
            # Record history
            avg_loss = epoch_loss / len(train_loader.dataset)
            avg_recon = epoch_recon / len(train_loader.dataset)
            avg_kld = epoch_kld / len(train_loader.dataset)
            
            self.history['loss'].append(avg_loss)
            self.history['recon_loss'].append(avg_recon)
            self.history['kld_loss'].append(avg_kld)
            
            if epoch % 20 == 0:
                logger.info(f'Epoch {epoch}: Loss={avg_loss:.4f}, Recon={avg_recon:.4f}, KLD={avg_kld:.4f}')
        
        logger.info("Training completed!")
        return self.history
    
    def get_team_embeddings(self, team_data: np.ndarray) -> np.ndarray:
        """Get embeddings for teams."""
        self.model.eval()
        with torch.no_grad():
            data_tensor = torch.FloatTensor(team_data).to(self.device)
            embeddings = self.model.get_embedding(data_tensor)
            return embeddings.cpu().numpy()
    
    def save_model(self, filepath: str):
        """Save the trained model."""
        torch.save({
            'model_state_dict': self.model.state_dict(),
            'input_dim': self.model.input_dim,
            'embedding_dim': self.model.embedding_dim,
            'hidden_dims': self.model.hidden_dims,
            'history': self.history
        }, filepath)
        logger.info(f"Model saved to {filepath}")

def create_and_train_embedding_model(feature_data: np.ndarray, 
                                   team_info: pd.DataFrame,
                                   save_path: str = "data/models/team_embeddings.pth") -> Tuple[VariationalTeamEmbedding, np.ndarray]:
    """
    Create and train a team embedding model.
    """
    
    logger.info("Creating and training team embedding model...")
    
    # Model parameters
    input_dim = feature_data.shape[1]
    embedding_dim = 32  # 32-dimensional embeddings
    hidden_dims = [64, 48]
    
    # Create model
    model = VariationalTeamEmbedding(input_dim, embedding_dim, hidden_dims)
    trainer = TeamEmbeddingTrainer(model)
    
    logger.info(f"Model architecture: {input_dim} -> {hidden_dims} -> {embedding_dim}")
    logger.info(f"Training on {feature_data.shape[0]} teams with {input_dim} features")
    
    # Train model
    history = trainer.train(
        feature_data,
        epochs=150,
        batch_size=64,
        learning_rate=1e-3,
        beta=0.5  # Lower beta to focus more on reconstruction
    )
    
    # Get embeddings
    team_embeddings = trainer.get_team_embeddings(feature_data)
    
    # Save model
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    trainer.save_model(save_path)
    
    # Save embeddings with team info
    embedding_data = {
        'teams': team_info.to_dict('records'),
        'embeddings': team_embeddings.tolist(),
        'embedding_dim': embedding_dim
    }
    
    embedding_path = save_path.replace('.pth', '_embeddings.json')
    with open(embedding_path, 'w') as f:
        json.dump(embedding_data, f, indent=2)
    
    logger.info("Team embeddings trained and saved!")
    logger.info(f"Embedding shape: {team_embeddings.shape}")
    
    return model, team_embeddings

def main():
    """Main training script for team embeddings."""
    print("Step 3: Team Embedding Model (VME)")
    print("=" * 35)
    
    # Load processed features
    print("Loading processed team features...")
    try:
        # Load feature matrix
        with open('data/raw/team_features_2024.json', 'r') as f:
            team_features_df = pd.read_json(f)
        
        print(f"Loaded features for {len(team_features_df)} teams")
        
        # Prepare data for embedding model
        import sys
        sys.path.append('src')
        from features.build_features import FootballFeatureEngineer
        
        engineer = FootballFeatureEngineer()
        
        # Get embedding features
        X_normalized, feature_names, team_info = engineer.prepare_embedding_features(team_features_df)
        
        print(f"Using {len(feature_names)} features for embeddings")
        
        # Train embedding model
        print("Training team embedding model...")
        model, embeddings = create_and_train_embedding_model(
            X_normalized, 
            team_info, 
            "data/models/team_embeddings.pth"
        )
        
        # Display some results
        print("\nSample Team Embeddings:")
        for i in range(min(5, len(team_info))):
            team_name = team_info.iloc[i]['team']
            embedding_preview = embeddings[i][:5]
            print(f"  {team_name}: [{', '.join(f'{x:.3f}' for x in embedding_preview)}, ...]")
        
        print("\nStep 3 Complete! Ready for Step 4: Prediction Head Model!")
        
        return model, embeddings, team_info, feature_names
        
    except Exception as e:
        print(f"Error in embedding training: {e}")
        logger.error(f"Embedding training error: {e}")
        return None

if __name__ == "__main__":
    main()