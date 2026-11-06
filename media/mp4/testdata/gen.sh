#!/bin/bash
# Writes the MP4 files the tests read, and what ffprobe says of each, with
# ffmpeg 7.1 as Debian trixie has it (libx264, libx265, libaom, libopus):
#
#   docker run --rm -v "$PWD":/w -w /w golang:1.27 bash -c \
#       'apt-get update -qq && apt-get install -y -qq ffmpeg >/dev/null && ./gen.sh'
#
# from media/mp4/testdata. Each file is a second of a 64x48 test pattern at
# 25 fps with B-frames (but AV1) and a GOP of 10, and a mono 48 kHz tone;
# every codec pair of H.264, H.265 and AV1 with AAC and Opus, the moov at
# the front (-movflags +faststart) for some and at the back for the others.
# ffmpeg writes edit lists for the B-frames' delay and AAC's priming
# samples; h264_aac_delay.mp4 starts its audio 0.2 s late, an empty edit,
# and h265_opus.mp4 has negative composition offsets (ctts version 1).
#
# <name>.probe has a line for each stream ("stream|index=..."), for each
# packet as ffprobe reads it without the edit list ("raw|..."), which is
# the sample table as the file has it, and as it reads it with the edit
# list ("pkt|..."), which is what a player presents.
set -euo pipefail

video_h264=(-c:v libx264 -preset veryfast -bf 2 -g 10 -keyint_min 10 -sc_threshold 0)
video_h265=(-c:v libx265 -preset veryfast -tag:v hvc1 -x265-params bframes=2:keyint=10:min-keyint=10:scenecut=0:log-level=error)
video_av1=(-c:v libaom-av1 -cpu-used 8 -g 10 -keyint_min 10 -b:v 50k)
audio_aac=(-c:a aac -b:a 24k)
audio_opus=(-c:a libopus -b:a 16k)

make() {
	# The audio input's options (an offset), then the movflags.
	local name=$1 video=$2 audio=$3 layout=$4 audio_in=${5:-} movflags=${6:-}
	local -n v=video_$video a=audio_$audio
	if [ "$layout" = front ]; then
		movflags+=+faststart
	fi
	local flags=()
	if [ -n "$movflags" ]; then
		flags=(-movflags "$movflags")
	fi
	# shellcheck disable=SC2086 # audio_in is split into options
	ffmpeg -hide_banner -loglevel error -y \
		-f lavfi -i testsrc2=size=64x48:rate=25 $audio_in -f lavfi -i sine=frequency=440:sample_rate=48000 \
		-t 1 -pix_fmt yuv420p "${v[@]}" -ac 1 -ar 48000 "${a[@]}" "${flags[@]}" "$name.mp4"
	{
		ffprobe -v error -show_data_hash SHA256 -of compact=p=0 \
			-show_entries stream=index,codec_name,codec_tag_string,width,height,sample_rate,channels,time_base,nb_frames,extradata_hash "$name.mp4" | sed 's/^/stream|/'
		ffprobe -v error -ignore_editlist 1 -of compact=p=0 \
			-show_entries packet=stream_index,pts,dts,duration,size,pos,flags "$name.mp4" | sed 's/^/raw|/'
		ffprobe -v error -of compact=p=0 \
			-show_entries packet=stream_index,pts,dts,pos "$name.mp4" | sed 's/^/pkt|/'
	} >"$name.probe"
}

make h264_aac h264 aac back
make h264_opus h264 opus front
make h265_aac h265 aac front
make h265_opus h265 opus back "" +negative_cts_offsets
make av1_aac av1 aac back
make av1_opus av1 opus front
make h264_aac_delay h264 aac front "-itsoffset 0.2"
ls -l ./*.mp4 ./*.probe
