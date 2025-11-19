package config

// NetworkFamily represents a family/category of networks that share similar behaviors
type NetworkFamily string

const (
	NetworkFamilyDefault           NetworkFamily = "default"
	NetworkFamilyAztec             NetworkFamily = "aztec"
	NetworkFamilyWaku              NetworkFamily = "waku"
	NetworkFamilyEthereumConsensus NetworkFamily = "ethereum_consensus"
)

// GetNetworkFamily returns the network family for a given network.
// Networks within the same family share the same behavior implementation.
func GetNetworkFamily(network Network) NetworkFamily {
	switch network {
	case NetworkAztecTestnet, NetworkAztecMainnet:
		return NetworkFamilyAztec

	case NetworkWakuStatus, NetworkWakuTWN:
		return NetworkFamilyWaku

	case NetworkEthCons, NetworkHolesky, NetworkPortal, NetworkGnosis:
		return NetworkFamilyEthereumConsensus

	default:
		return NetworkFamilyDefault
	}
}

// IsAztecNetwork returns true if the network is part of the Aztec family
func IsAztecNetwork(network Network) bool {
	return GetNetworkFamily(network) == NetworkFamilyAztec
}

// IsWakuNetwork returns true if the network is part of the Waku family
func IsWakuNetwork(network Network) bool {
	return GetNetworkFamily(network) == NetworkFamilyWaku
}

// IsEthereumConsensusNetwork returns true if the network is part of the Ethereum Consensus family
func IsEthereumConsensusNetwork(network Network) bool {
	return GetNetworkFamily(network) == NetworkFamilyEthereumConsensus
}


