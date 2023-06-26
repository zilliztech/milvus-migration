package source

type ChannelSource struct {
	ESSource *ESSource
}

func NewChannelSource(esSouce *ESSource) *ChannelSource {
	return &ChannelSource{
		ESSource: esSouce,
	}
}
