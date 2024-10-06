#!/bin/bash

# This script installs ZSH, Git, Curl, and pre-commit.
# It is used to set up the development environment for the project.
# ZSH is a powerful shell with additional features and customization options.
# Usage: ./setup-zsh.sh

# Install ZSH, Git, Curl, and pre-commit
apt-get update && apt-get install -y zsh fonts-powerline

# Install Oh My Zsh
sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended

# Clone necessary plugins and theme
git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting
git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k

# Configure .zshrc
echo "ZSH_THEME='powerlevel10k/powerlevel10k'" >> $HOME/.zshrc
echo "source $HOME/.oh-my-zsh/custom/themes/powerlevel10k/powerlevel10k.zsh-theme" >> $HOME/.zshrc
echo "source ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting/zsh-syntax-highlighting.zsh" >> $HOME/.zshrc
echo "source ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-autosuggestions/zsh-autosuggestions.zsh" >> $HOME/.zshrc
# Add configuration to skip the wizard
echo 'POWERLEVEL9K_DISABLE_CONFIGURATION_WIZARD=true' >> ~/.zshrc

# Copy existing .p10k.zsh configuration if it exists
if [ -f /workspace/.devcontainer/.p10k.zsh ]; then
    echo "Using existing .p10k.zsh configuration."
    echo "source /workspace/.devcontainer/.p10k.zsh" >> ~/.zshrc
else
    echo "Please configure Powerlevel10k interactively or manually edit the .p10k.zsh file."
fi