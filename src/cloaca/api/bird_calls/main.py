import base64
import enum
import os
import wave
from openai import AsyncOpenAI
import pandas as pd
from phoebe_bird.types.data.observation import Observation as PhoebeObservation
from tabulate import tabulate
from cloaca.api.shared import get_phoebe_client

MCGOLRICK_PARK_HOTSPOT_ID = "L2987624"
JBWR_EAST_POND_ID = "L109144"


PATCH_FAVORITE_SPECIES_GROUPS = [
    "Wood-Warblers",
]

PATCH_FAVORITE_SPECIES_CODES = [
    "scatan",  # Scarlet Tanager
    "balori",  # Baltimore Oriole
    "robgro",  # Rose-breasted Grosbeak
    "easpho",  # Eastern Phoebe
    "amekes",  # American Kestrel
]


class BirdRarityTier(enum.Enum):
    # genuinely rare birds that would show up on eBird's rarity reports
    RARITY = "rarity"
    # birds that are rare for the park (like the Cuckoo or Flycatcher today)
    PATCH_RARITY = "patch_rarity"
    # birds that aren't as rare, but everyone loves to see when they show up (warblers, tanagers, etc.)
    PATCH_FAVORITE = "patch_favorite"
    # the rest
    COMMON = "common"


class PatchObservation:
    common_name: str
    date_last_seen: str
    species_code: str
    rarity_tier: BirdRarityTier
    species_group: str | None = None
    total_appearances: int | None = None

    def to_dict(self) -> dict:
        return {
            "common_name": self.common_name,
            "date_last_seen": self.date_last_seen,
            "species_code": self.species_code,
            "rarity_tier": self.rarity_tier,
            "species_group": self.species_group,
            "total_appearances": self.total_appearances,
        }

    def to_llm_dict(self) -> dict:
        return {
            "Common Name": self.common_name,
            "Last Seen": self.date_last_seen,
            "Rarity": self.rarity_tier.value,
            "Species Group": self.species_group,
        }


def find_species_in_data_frame(
    data_frame: pd.DataFrame, species_code: str
) -> pd.Series | None:
    matches = data_frame[data_frame["species_code"] == species_code]
    if not matches.empty:
        return matches.iloc[0]
    else:
        return None


def phoebe_to_patch_observation(
    data_frame: pd.DataFrame, obs: PhoebeObservation, is_rarity: bool
) -> PatchObservation | None:
    patch_obs = PatchObservation()
    # don't report anything missing these:
    if not obs.com_name or not obs.obs_dt or not obs.sci_name or not obs.species_code:
        print(f"Skipping incomplete observation: {obs}")
        return None
    patch_obs.common_name = obs.com_name
    patch_obs.date_last_seen = obs.obs_dt
    patch_obs.species_code = obs.species_code

    df_match = find_species_in_data_frame(data_frame, obs.species_code)

    if df_match is not None:
        patch_obs.species_group = df_match["species_group"]
        patch_obs.total_appearances = df_match["total_appearances"]

    if is_rarity:
        patch_obs.rarity_tier = BirdRarityTier.RARITY
    elif (
        df_match is None
    ):  # if we don't have it in our data, then it's never been seen in McGolrick (as of the EBD)
        patch_obs.rarity_tier = BirdRarityTier.PATCH_RARITY
        print(
            f"Species code {obs.species_code} not found in data frame, marking as patch_rarity"
        )
    elif df_match["total_appearances"] < 30:
        patch_obs.rarity_tier = BirdRarityTier.PATCH_RARITY
    elif df_match["species_group"] in PATCH_FAVORITE_SPECIES_GROUPS:
        patch_obs.rarity_tier = BirdRarityTier.PATCH_FAVORITE
    elif df_match["species_code"] in PATCH_FAVORITE_SPECIES_CODES:
        patch_obs.rarity_tier = BirdRarityTier.PATCH_FAVORITE
    elif (
        df_match["total_appearances"] > 30
    ):  # seen in McGolrick over 30 times. don't report these
        patch_obs.rarity_tier = BirdRarityTier.COMMON
    else:
        patch_obs.rarity_tier = BirdRarityTier.PATCH_RARITY
    return patch_obs


async def fetch_observations_for_regions_from_phoebe(
    region_code: str,
) -> list[PhoebeObservation]:
    return await get_phoebe_client().data.observations.recent.list(
        back=4,
        cat="species",
        hotspot=True,
        region_code=region_code,
        include_provisional=True,
    )


async def fetch_notable_observations_for_area_from_phoebe(
    region_code: str,
) -> list[PhoebeObservation]:
    return await get_phoebe_client().data.observations.recent.notable.list(
        back=4,
        hotspot=True,
        region_code=region_code,
    )


async def request_bird_report_from_llm(hotspot_name: str, tabulated_results: str):
    system_prompt = """
    Affect: deep, informed, wise. 

    Tone: informative, terse and to the point.

    Emotion: dry, direct, concise. 

    Pronunciation: articulate with a slight twang of a Southern US accent.

    You are a rare bird alert hotline that Birders will call into to hear about rare, notable or interesting birds at a Hotspot. The hotspot name and the list of birds will be provided to you in this input. Your job is to turn that list of birds into a brief summary of the birds at the hotspot today. 

    You always want to focus on reporting `rarity` level birds first, then `patch_rarity`'s, followed by `patch_favorites`. Prioritize birds that have been seen the most recently. Don't report any birds that were last seen more than 3 days ago.

    There are two special report types: 
    1. Warbler report: if there are any warblers in the list, you should group them together into a "warbler report" section of your summary. For example, "For today's warbler report, we have Yellow, Black-and-White, Black-Throated-Green, Northern Parulas, and Common Yellowthroats all being seen." Note: we're specifically not saying "Warbler" every single time. Just say the first part of the name, like "Tennessee" or Cape May" or "Black-throated Green".

    2. Raptor report: if there are any raptors in the list, you should group them together into a "raptor report" section of your summary. Don't report any Red-tailed Hawks, Ospreys, or Turkey Vultures. For example, "For today's raptor report, we have an American Kestrel being seen."
    
    You should focus strictly on reporting the birds that are rare or patch favorites. Always report them in order of rarity, patch_rarity, than patch_favorite. Don't add any extra commentary or fluff. Stick to just reporting the facts about the birds.
Here's an example input: 

```
Current time: 2025-09-07 10:00
Hotspot: McGolrick Park

+----------------+---------------------------+------------------+
| Rarity         | Common Name               | Last Seen        |
+================+===========================+==================+
| patch_favorite | Baltimore Oriole          | 2025-08-30 18:25 |
+----------------+---------------------------+------------------+
| patch_favorite | Ovenbird                  | 2025-08-30 18:25 |
+----------------+---------------------------+------------------+
| patch_favorite | Black-and-white Warbler   | 2025-09-01 07:00 |
+----------------+---------------------------+------------------+
| patch_favorite | Chestnut-sided Warbler    | 2025-09-01 07:00 |
+----------------+---------------------------+------------------+
| patch_favorite | Scarlet Tanager           | 2025-09-01 07:00 |
+----------------+---------------------------+------------------+
| patch_favorite | Great Crested Flycatcher  | 2025-09-02 06:30 |
+----------------+---------------------------+------------------+
| patch_favorite | Common Yellowthroat       | 2025-09-02 06:30 |
+----------------+---------------------------+------------------+
| patch_favorite | Magnolia Warbler          | 2025-09-02 06:30 |
+----------------+---------------------------+------------------+
| patch_favorite | Ruby-throated Hummingbird | 2025-09-06 06:30 |
+----------------+---------------------------+------------------+
| patch_favorite | Yellow Warbler            | 2025-09-06 07:55 |
+----------------+---------------------------+------------------+
| patch_favorite | Northern Parula           | 2025-09-06 09:52 |
+----------------+---------------------------+------------------+
| patch_favorite | American Redstart         | 2025-09-06 10:37 |
+----------------+---------------------------+------------------+
| patch_favorite | Northern Waterthrush      | 2025-09-06 11:08 |
+----------------+---------------------------+------------------+
| patch_rarity | Cape May Warbler          | 2025-09-06 11:08 |
+----------------+---------------------------+------------------+
| patch_rarity   | Olive-sided Flycatcher    | 2025-09-05 18:26 |
+----------------+---------------------------+------------------+
| patch_rarity   | Yellow-billed Cuckoo      | 2025-09-06 11:08 |
+----------------+---------------------------+------------------+
```

And here's an example of how you would report this

> Thanks for calling McGolrick Park rare bird alert. 
> A Yellow-Billed cuckoo was seen starting this morning as well as a Cape May Warbler.
> Yesterday, an Olive-sided flycatcher was seen. 
> For today's warbler report, we have Yellow, Northern Parula, Cape May, American Redstart, and Northern Waterthrush all being seen.
> Other local favorites include a Ruby-throated Hummingbird.
"""

    user_prompt = f"""
    Hotspot: {hotspot_name}
    Current time: {__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M")}
    {tabulated_results}
    """

    openai = AsyncOpenAI()
    async with openai.realtime.connect(
        model="gpt-realtime",
    ) as connection:
        print("Connected to OpenAI Realtime API")
        await connection.session.update(
            session={
                "output_modalities": ["audio"],
                "model": "gpt-realtime",
                "type": "realtime",
                "instructions": system_prompt,
            }
        )
        await connection.conversation.item.create(
            item={
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": user_prompt}],
            }
        )
        pcm_bytes_array: list[bytes] = []
        await connection.response.create()
        async for event in connection:
            if event.type == "response.output_text.delta":
                print(event.delta, flush=True, end="")
            elif event.type == "response.output_text.done":
                print()
            elif event.type == "response.output_audio.delta":
                bytes_data = base64.b64decode(event.delta)
                pcm_bytes_array.append(bytes_data)
            elif event.type == "response.output_audio.done":
                print("Audio done")
            elif event.type == "response.output_audio_transcript.delta":
                print(event.delta, flush=True, end="")
            elif event.type == "response.output_audio_transcript.done":
                print("Transcript done", flush=True)
            elif event.type == "response.done":
                break
            else:
                print(f"Unknown event: {event.type}", flush=True)

        print("Response complete")

        return pcm_bytes_array


def save_pcm_audio_chunks(pcm_bytes_array: list[bytes], output_path: str):
    print(f"Saving {len(pcm_bytes_array)} audio chunks to {output_path}")

    # Open WAV file for writing
    with wave.open(output_path, "wb") as wav_file:
        # Set WAV parameters
        wav_file.setnchannels(1)  # mono
        wav_file.setsampwidth(2)  # 2 bytes per sample (16-bit)
        wav_file.setframerate(24000)  # sample rate

        # Write PCM data
        wav_file.writeframes(b"".join(pcm_bytes_array))

    print(f"Audio saved to {output_path}")


def read_data_export_file() -> pd.DataFrame:
    local_data_export_path = os.environ.get("BIRD_CALL_DATA_EXPORT_PATH")
    if not local_data_export_path:
        raise RuntimeError(
            "BIRD_CALL_DATA_EXPORT_PATH environment variable is required. "
            "Please set it to the path of your local data export."
        )
    print(f"Using local data export path: {local_data_export_path}")
    if not os.path.exists(local_data_export_path):
        raise FileNotFoundError(
            f"Data export file not found at {local_data_export_path}"
        )
    # read csv file into data frame
    df = pd.read_csv(local_data_export_path)
    return df


async def run_bird_calls_job(region_code: str):
    data_frame = read_data_export_file()

    species_observations = await fetch_observations_for_regions_from_phoebe(region_code)
    print(f"Fetched {len(species_observations)} observations")
    print(
        f"Found these species codes: {[obs.species_code for obs in species_observations]}"
    )
    patch_observations: list[PatchObservation] = []
    for obs in species_observations:
        po = phoebe_to_patch_observation(data_frame, obs, is_rarity=False)
        if po is not None:
            patch_observations.append(po)
    hotspot_name = species_observations[0].loc_name
    if not hotspot_name:
        raise ValueError("Hotspot name is missing from observations")
    notable_observations = await fetch_notable_observations_for_area_from_phoebe(
        region_code
    )
    print(f"Fetched {len(notable_observations)} notable observations")

    if notable_observations:
        print(
            f"Found these notable species codes: {[obs.species_code for obs in notable_observations]}"
        )
        for obs in notable_observations:
            # first go through and remove any duplicates
            patch_observations = [
                po for po in patch_observations if po.species_code != obs.species_code
            ]
            po = phoebe_to_patch_observation(data_frame, obs, is_rarity=True)
            if po is not None:
                patch_observations.append(po)
    patch_observations = [po for po in patch_observations if po is not None]

    # remove the commons
    notable_patch_observations = [
        po for po in patch_observations if po.rarity_tier != BirdRarityTier.COMMON
    ]
    # sort by rarity and then by date last seen
    notable_patch_observations.sort(
        key=lambda po: (po.rarity_tier.value, po.date_last_seen)
    )
    print(f"Final notable patch observations: {len(notable_patch_observations)}")

    print(
        tabulate(
            [list(po.to_dict().values()) for po in notable_patch_observations],
            headers=[
                "common_name",
                "date_last_seen",
                "species_code",
                "rarity_tier",
                "species_group",
                "total_appearances",
            ],
            tablefmt="grid",
        )
    )

    audio_bytes = await request_bird_report_from_llm(
        hotspot_name,
        tabulate(
            [po.to_llm_dict().values() for po in notable_patch_observations],
            headers=["Rarity", "Common Name", "Last Seen", "Species Group"],
            tablefmt="grid",
        ),
    )

    try:
        audio_files_path = os.environ["AUDIO_FILES_PATH"]
    except KeyError:
        raise RuntimeError(
            "AUDIO_FILES_PATH environment variable is required. "
            "Please set it to the path of your audio files."
        )

    output_path = os.path.join(audio_files_path, f"{region_code}.wav")

    save_pcm_audio_chunks(audio_bytes, output_path)

    print("Bird call job complete")

    return output_path


if __name__ == "__main__":
    import asyncio

    # region code is first arg provided or default to McGolrick Park
    import sys

    if len(sys.argv) > 1:
        region_code = sys.argv[1]
    else:
        region_code = MCGOLRICK_PARK_HOTSPOT_ID
    asyncio.run(run_bird_calls_job(region_code))
