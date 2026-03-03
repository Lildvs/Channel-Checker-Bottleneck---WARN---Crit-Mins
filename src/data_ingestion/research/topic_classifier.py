"""Topic classifier for research papers using keyword matching."""

import re
from dataclasses import dataclass

from src.data_ingestion.research.base import ResearchPaper


@dataclass
class TopicMatch:
    """Represents a topic match with confidence score."""

    topic: str
    confidence: float
    matched_keywords: list[str]


TOPIC_KEYWORDS: dict[str, list[str]] = {
    "energy": [
        "battery", "batteries", "lithium", "lithium-ion", "li-ion",
        "solid-state battery", "energy storage", "grid storage",
        "supercapacitor", "flow battery", "sodium-ion",
        "solar", "photovoltaic", "pv cell", "perovskite solar",
        "solar panel", "solar cell", "solar efficiency",
        "nuclear", "fusion", "fission", "tokamak", "iter",
        "nuclear reactor", "small modular reactor", "smr",
        "hydrogen", "fuel cell", "green hydrogen", "electrolyzer",
        "hydrogen production", "hydrogen storage", "h2",
        "power grid", "smart grid", "renewable energy", "wind power",
        "wind turbine", "offshore wind", "energy transition",
        "decarbonization", "carbon capture", "ccs", "ccus",
    ],
    "semiconductors": [
        "semiconductor", "semiconductors", "transistor", "mosfet",
        "cmos", "chip", "microchip", "integrated circuit", "ic",
        "lithography", "euv", "extreme ultraviolet", "etching",
        "fab", "foundry", "wafer", "photoresist", "nanometer process",
        "3nm", "5nm", "7nm", "tsmc", "asml",
        "asic", "fpga", "soc", "system-on-chip", "gpu", "cpu",
        "memory chip", "dram", "nand", "flash memory",
        "moore's law", "chiplet", "advanced packaging", "3d ic",
        "gaafet", "finfet", "power semiconductor",
    ],
    "ai_ml": [
        "neural network", "deep learning", "deep neural",
        "convolutional neural", "cnn", "recurrent neural", "rnn",
        "transformer", "attention mechanism", "self-attention",
        "large language model", "llm", "gpt", "generative ai",
        "foundation model", "language model", "chatgpt",
        "instruction tuning", "rlhf", "fine-tuning",
        "machine learning", "supervised learning", "unsupervised",
        "reinforcement learning", "rl", "gradient descent",
        "backpropagation", "training data", "inference",
        "computer vision", "image recognition", "object detection",
        "natural language processing", "nlp", "speech recognition",
        "recommendation system", "autonomous driving",
        "model training", "distributed training", "gpu cluster",
        "tpu", "neural architecture", "automl",
    ],
    "materials": [
        "graphene", "carbon nanotube", "cnt", "nanomaterial",
        "nanoparticle", "quantum dot", "2d material", "mos2",
        "alloy", "high-entropy alloy", "superalloy", "titanium alloy",
        "aluminum alloy", "steel", "metallurgy",
        "rare earth", "rare-earth element", "ree", "cobalt",
        "lithium", "nickel", "neodymium", "dysprosium",
        "superconductor", "room temperature superconductor",
        "metamaterial", "smart material", "shape memory",
        "piezoelectric", "thermoelectric",
        "polymer", "biopolymer", "composite", "carbon fiber",
        "ceramic", "glass", "coating",
    ],
    "biotech": [
        "crispr", "cas9", "cas12", "cas13", "gene editing",
        "gene therapy", "genome editing", "genetic engineering",
        "mrna", "mrna vaccine", "lipid nanoparticle", "lnp",
        "vaccine", "immunotherapy", "antibody", "monoclonal",
        "protein", "protein folding", "alphafold", "enzyme",
        "antibody", "biologic", "biosimilar",
        "car-t", "cell therapy", "stem cell", "regenerative",
        "drug discovery", "clinical trial", "fda approval",
        "pharmaceutical", "drug development", "biomarker",
        "precision medicine", "personalized medicine",
        "pcr", "sequencing", "genomics", "proteomics",
        "diagnostics", "biosensor",
    ],
    "agriculture": [
        "crop", "crop yield", "harvest", "grain", "wheat",
        "corn", "soybean", "rice", "cotton",
        "fertilizer", "nitrogen fertilizer", "phosphate",
        "potash", "pesticide", "herbicide", "fungicide",
        "seed", "hybrid seed", "gmo", "genetically modified",
        "precision agriculture", "precision farming", "agtech",
        "vertical farming", "hydroponics", "aeroponics",
        "drone agriculture", "agricultural robotics",
        "irrigation", "soil", "soil health", "water management",
        "drought", "food security", "food supply",
        "livestock", "cattle", "poultry", "aquaculture",
        "animal feed", "meat production",
    ],
    "supply_chain": [
        "logistics", "supply chain", "freight", "shipping",
        "transportation", "last mile", "warehouse",
        "distribution", "fulfillment",
        "inventory", "inventory management", "stockout",
        "safety stock", "lead time", "just-in-time", "jit",
        "manufacturing", "factory", "production", "assembly",
        "automation", "industrial", "plant capacity",
        "import", "export", "trade", "tariff", "customs",
        "port", "container", "shipping route",
        "supply shortage", "bottleneck", "disruption",
        "procurement", "supplier", "vendor", "sourcing",
        "reshoring", "nearshoring", "onshoring",
    ],
    "quantum": [
        "qubit", "quantum bit", "quantum state", "superposition",
        "entanglement", "quantum entanglement", "coherence",
        "decoherence",
        "quantum computing", "quantum computer", "quantum processor",
        "quantum supremacy", "quantum advantage",
        "quantum algorithm", "quantum circuit",
        "superconducting qubit", "trapped ion", "photonic quantum",
        "topological qubit", "quantum dot qubit",
        "quantum error correction", "error-corrected",
        "fault-tolerant quantum", "logical qubit",
        "quantum cryptography", "quantum key distribution", "qkd",
        "quantum sensing", "quantum simulation",
        "quantum machine learning", "quantum optimization",
    ],

    "telecom_connectivity": [
        "5g", "6g", "5g network", "6g network", "millimeter wave", "mmwave",
        "massive mimo", "beamforming", "network slicing", "open ran", "o-ran",
        "leo satellite", "low earth orbit", "starlink", "oneweb", "kuiper",
        "satellite constellation", "satellite internet", "vsat",
        "fiber optic", "fiber optics", "ftth", "fttx", "optical fiber",
        "submarine cable", "undersea cable", "dark fiber",
        "iot", "internet of things", "mesh network", "lorawan", "zigbee",
        "nb-iot", "narrowband iot", "lpwan", "sigfox",
        "spectrum allocation", "spectrum auction", "radio frequency",
        "rf spectrum", "wireless spectrum", "frequency band",
        "c-band", "mid-band", "sub-6ghz",
    ],
    "cloud_edge_compute": [
        "data center", "datacenter", "hyperscale", "colocation", "colo",
        "server farm", "data hall", "tier 4 datacenter",
        "gpu cluster", "hpc", "high performance computing", "supercomputer",
        "nvidia a100", "h100", "tensor core", "compute cluster",
        "ai accelerator", "tpu", "inference chip",
        "edge computing", "edge node", "edge device", "mec",
        "multi-access edge", "fog computing", "cloudlet",
        "edge inference", "on-device ai",
        "serverless", "lambda function", "faas", "function as a service",
        "kubernetes", "k8s", "container orchestration", "docker",
        "microservices", "cloud native", "iaas", "paas", "saas",
    ],
    "cybersecurity_infosec": [
        "post-quantum cryptography", "pqc", "lattice-based cryptography",
        "quantum-resistant", "kyber", "dilithium", "sphincs",
        "zero trust", "zero trust architecture", "zta", "ztna",
        "microsegmentation", "identity-based security", "sase",
        "network security", "firewall", "intrusion detection", "ids",
        "intrusion prevention", "ips", "siem", "soar",
        "threat intelligence", "vulnerability management",
        "malware analysis", "ransomware", "threat hunting", "apt",
        "advanced persistent threat", "digital forensics", "incident response",
        "reverse engineering", "exploit", "zero-day", "cve",
        "encryption", "tls", "ssl", "end-to-end encryption", "e2ee",
        "homomorphic encryption", "secure enclave", "hsm",
    ],

    "robotics_autonomy": [
        "humanoid robot", "humanoid", "bipedal robot", "atlas robot",
        "optimus", "figure 01", "digit robot",
        "industrial robot", "cobot", "collaborative robot", "robot arm",
        "robotic arm", "end effector", "pick and place", "palletizing",
        "fanuc", "kuka", "abb robot", "universal robots",
        "swarm robotics", "multi-robot", "robot swarm", "swarm intelligence",
        "distributed robotics", "robot coordination",
        "drone", "uav", "unmanned aerial", "quadcopter", "multirotor",
        "drone delivery", "drone logistics", "autonomous drone",
        "exoskeleton", "powered exoskeleton", "wearable robot",
        "assistive robotics", "rehabilitation robot",
    ],
    "aerospace_defense": [
        "hypersonic", "hypersonic missile", "scramjet", "mach 5",
        "hypersonic glide vehicle", "hgv", "boost-glide",
        "reusable launch", "rocket reuse", "starship", "falcon 9",
        "new glenn", "vulcan", "ariane", "rocket propulsion",
        "orbital launch", "space launch", "rocket engine",
        "orbital manufacturing", "space manufacturing", "in-space assembly",
        "space station", "iss", "lunar gateway", "artemis program",
        "directed energy", "laser weapon", "high energy laser", "hel",
        "microwave weapon", "emp", "electromagnetic pulse",
        "electronic warfare", "ew", "jamming", "signal intelligence",
        "sigint", "elint", "radar jamming", "spoofing",
        "counter-drone", "c-uas",
    ],
    "autonomous_vehicles": [
        "lidar", "light detection", "velodyne", "luminar", "ouster",
        "solid-state lidar", "radar sensor", "automotive radar",
        "v2x", "vehicle to everything", "v2v", "vehicle to vehicle",
        "v2i", "vehicle to infrastructure", "dsrc", "c-v2x",
        "path planning", "motion planning", "trajectory planning",
        "autonomous navigation", "slam", "simultaneous localization",
        "obstacle avoidance", "lane keeping",
        "sensor fusion", "perception system", "multi-sensor fusion",
        "camera lidar fusion", "perception stack",
        "electric powertrain", "ev platform", "skateboard platform",
        "drive-by-wire", "steer-by-wire", "brake-by-wire",
        "level 4 autonomy", "level 5 autonomy", "full self-driving",
        "autonomous taxi", "robotaxi", "waymo", "cruise", "argo",
    ],

    "fintech_digital_assets": [
        "blockchain", "distributed ledger", "dlt", "consensus mechanism",
        "proof of stake", "proof of work", "layer 2", "rollup",
        "cbdc", "central bank digital currency", "digital dollar",
        "digital euro", "digital yuan", "e-cny",
        "defi", "decentralized finance", "yield farming", "liquidity pool",
        "automated market maker", "amm", "dex", "decentralized exchange",
        "lending protocol", "stablecoin", "dai", "usdc",
        "smart contract", "solidity", "ethereum", "solana", "polygon",
        "web3", "dapp", "decentralized application",
        "tokenization", "security token", "asset tokenization", "nft",
        "real world assets", "rwa", "fractional ownership",
        "algorithmic trading", "algo trading", "high frequency trading",
        "hft", "market making", "quantitative trading", "quant",
    ],
    "macro_geoeconomics": [
        "trade flow", "trade balance", "current account", "trade deficit",
        "trade surplus", "export data", "import data", "bilateral trade",
        "trade agreement", "free trade", "wto",
        "sanctions", "economic sanctions", "ofac", "sanctions regime",
        "sanctions evasion", "secondary sanctions", "export controls",
        "entity list", "blacklist",
        "monetary policy", "central bank", "federal reserve", "ecb",
        "interest rate", "quantitative easing", "qe", "quantitative tightening",
        "inflation targeting", "yield curve control", "ycc",
        "sovereign debt", "government bond", "treasury", "gilt",
        "bund", "sovereign default", "debt crisis", "fiscal policy",
        "debt to gdp", "fiscal deficit",
        "reserve currency", "dollar hegemony", "dedollarization",
        "petrodollar", "currency reserves", "forex reserves",
        "swift", "cross-border payment", "brics currency",
    ],

    "advanced_manufacturing": [
        "3d printing", "additive manufacturing", "selective laser sintering",
        "sls", "selective laser melting", "slm", "stereolithography", "sla",
        "fused deposition", "fdm", "binder jetting", "metal 3d printing",
        "nanomanufacturing", "nanofabrication", "molecular manufacturing",
        "self-assembly", "directed self-assembly", "dsa",
        "atomic layer deposition", "ald",
        "digital twin", "virtual twin", "simulation model", "digital thread",
        "product lifecycle", "plm", "model-based systems",
        "generative design", "topology optimization", "parametric design",
        "computational design", "design automation", "cad cam",
        "smart factory", "industry 4.0", "industrial iot", "iiot",
        "connected factory", "manufacturing execution", "mes",
    ],
    "photonics_optics": [
        "laser", "fiber laser", "solid-state laser", "ultrafast laser",
        "femtosecond laser", "high-power laser", "laser diode",
        "vcsel", "quantum cascade laser", "qcl",
        "optical computing", "photonic computing", "optical processor",
        "optical neural network", "photonic accelerator",
        "all-optical", "optical interconnect",
        "metamaterial", "negative index", "plasmonic", "metasurface",
        "optical metamaterial", "acoustic metamaterial",
        "silicon photonics", "photonic integrated circuit", "pic",
        "optical transceiver", "optical modulator", "photodetector",
        "waveguide", "arrayed waveguide grating", "awg",
        "holographic display", "holography", "volumetric display",
        "spatial light modulator", "slm", "augmented reality optics",
        "microled", "oled", "display technology",
    ],
    "neurotech_bci": [
        "brain-computer interface", "bci", "brain-machine interface", "bmi",
        "neural interface", "neuralink", "invasive bci", "non-invasive bci",
        "eeg headset", "ecog", "electrocorticography",
        "neural implant", "brain implant", "deep brain stimulation", "dbs",
        "cochlear implant", "retinal implant", "neural prosthesis",
        "implantable electrode", "microelectrode array",
        "cognitive augmentation", "cognitive enhancement", "neurostimulation",
        "transcranial magnetic", "tms", "transcranial direct current", "tdcs",
        "neurofeedback", "brain training",
        "neuromorphic", "neuromorphic computing", "spiking neural network",
        "snn", "memristor", "intel loihi", "ibm truenorth",
        "brain-inspired computing", "neuromorphic chip",
        "neural decoding", "brain mapping", "connectome", "neural signal",
        "spike sorting", "neural recording", "optogenetics",
    ],
}


class TopicClassifier:
    """Classifies research papers into topics based on keyword matching.

    This is a speed-first classifier that uses keyword matching in
    title and abstract to quickly classify papers into one or more topics.
    """

    def __init__(
        self,
        min_confidence: float = 0.1,
        use_existing_topics: bool = True,
    ):
        """Initialize the classifier.

        Args:
            min_confidence: Minimum confidence threshold for a topic match
            use_existing_topics: Whether to include topics already on the paper
        """
        self.min_confidence = min_confidence
        self.use_existing_topics = use_existing_topics

        self._patterns: dict[str, list[tuple[re.Pattern, str]]] = {}
        for topic, keywords in TOPIC_KEYWORDS.items():
            patterns = []
            for keyword in keywords:
                pattern = re.compile(
                    r"\b" + re.escape(keyword) + r"\b",
                    re.IGNORECASE,
                )
                patterns.append((pattern, keyword))
            self._patterns[topic] = patterns

    def classify(self, paper: ResearchPaper) -> list[TopicMatch]:
        """Classify a paper into topics.

        Args:
            paper: ResearchPaper to classify

        Returns:
            List of TopicMatch objects sorted by confidence
        """
        text = f"{paper.title} {paper.abstract or ''}"

        matches = []

        for topic, patterns in self._patterns.items():
            matched_keywords = []
            total_matches = 0

            for pattern, keyword in patterns:
                keyword_matches = len(pattern.findall(text))
                if keyword_matches > 0:
                    matched_keywords.append(keyword)
                    total_matches += keyword_matches

            if matched_keywords:
                unique_ratio = len(matched_keywords) / len(patterns)
                frequency_factor = min(total_matches / 5, 1.0)  # Cap at 5 occurrences

                confidence = (unique_ratio * 0.7 + frequency_factor * 0.3)

                if confidence >= self.min_confidence:
                    matches.append(
                        TopicMatch(
                            topic=topic,
                            confidence=round(confidence, 3),
                            matched_keywords=matched_keywords[:5],  # Top 5 keywords
                        )
                    )

        if self.use_existing_topics and paper.topics:
            existing_topics = set(m.topic for m in matches)
            for topic in paper.topics:
                if topic not in existing_topics and topic in TOPIC_KEYWORDS:
                    matches.append(
                        TopicMatch(
                            topic=topic,
                            confidence=0.5,  # Medium confidence for source-provided
                            matched_keywords=["source_provided"],
                        )
                    )

        matches.sort(key=lambda m: m.confidence, reverse=True)

        return matches

    def classify_batch(
        self,
        papers: list[ResearchPaper],
    ) -> dict[str, list[TopicMatch]]:
        """Classify multiple papers efficiently.

        Args:
            papers: List of papers to classify

        Returns:
            Dict mapping paper source_id to list of TopicMatch
        """
        results = {}
        for paper in papers:
            results[paper.source_id] = self.classify(paper)
        return results

    def get_primary_topic(self, paper: ResearchPaper) -> str | None:
        """Get the primary (highest confidence) topic for a paper.

        Args:
            paper: ResearchPaper to classify

        Returns:
            Primary topic name or None
        """
        matches = self.classify(paper)
        return matches[0].topic if matches else None

    def get_all_topics(
        self,
        paper: ResearchPaper,
        min_confidence: float | None = None,
    ) -> list[str]:
        """Get all topics for a paper above confidence threshold.

        Args:
            paper: ResearchPaper to classify
            min_confidence: Override minimum confidence threshold

        Returns:
            List of topic names
        """
        threshold = min_confidence if min_confidence is not None else self.min_confidence
        matches = self.classify(paper)
        return [m.topic for m in matches if m.confidence >= threshold]

    def update_paper_topics(self, paper: ResearchPaper) -> ResearchPaper:
        """Update a paper's topics field with classified topics.

        Args:
            paper: ResearchPaper to update

        Returns:
            Paper with updated topics
        """
        topics = self.get_all_topics(paper)
        paper.topics = list(set(paper.topics + topics))
        return paper


_default_classifier: TopicClassifier | None = None


def get_classifier() -> TopicClassifier:
    """Get the default topic classifier instance."""
    global _default_classifier
    if _default_classifier is None:
        _default_classifier = TopicClassifier()
    return _default_classifier


def classify_paper(paper: ResearchPaper) -> list[TopicMatch]:
    """Convenience function to classify a paper with default classifier."""
    return get_classifier().classify(paper)
