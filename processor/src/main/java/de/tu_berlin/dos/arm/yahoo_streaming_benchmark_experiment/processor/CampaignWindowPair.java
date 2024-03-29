package de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.processor;

class CampaignWindowPair {
    String campaign;
    Window window;

    public CampaignWindowPair(String campaign, Window window) {
        this.campaign = campaign;
        this.window = window;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof CampaignWindowPair) {
            return campaign.equals(((CampaignWindowPair)other).campaign)
                    && window.equals(((CampaignWindowPair)other).window);
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = result * prime + campaign.hashCode();
        result = result * prime + window.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "{ " + campaign + " : " + window.toString() + " }";
    }
}
