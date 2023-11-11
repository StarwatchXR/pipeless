use std;
use std::str::FromStr;
use gstreamer as gst;
use gst::prelude::*;
use gstreamer_app as gst_app;
use log::{info, error, warn, debug};

use crate as pipeless;

#[derive(Debug)]
pub struct PipelineError;
impl std::error::Error for PipelineError {}
impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "An error ocurred with the output pipeline")
    }
}

// For us each pipeline contains a single strea (which can have audio + video+ ...)
#[derive(Clone)]
pub struct StreamDef {
    video: pipeless::config::video::Video,
    // Stores tags that the pipeline will have when created, if any.
    // Sometimes we get input tags before the ooutput pipeline is created
    initial_tags: Option<gst::tags::TagList>,
}
impl StreamDef {
    pub fn new(uri: String) -> Self {
        StreamDef {
            video: pipeless::config::video::Video::new(uri),
            initial_tags: None,
        }
    }

    pub fn set_initial_tags(&mut self, tags: gst::TagList) {
        self.initial_tags = Some(tags);
    }

    pub fn get_video(&self) -> &pipeless::config::video::Video {
        &self.video
    }
}

fn set_pipeline_tags(pipeline: &gst::Pipeline, new_tag_list: &gst::TagList) {
    debug!("Updating pipeline with new tags: {}", new_tag_list.to_string());
    // NOTE: We always must have an element called taginject on the pipeline
    match pipeline.by_name("taginject") {
        None => warn!("Taginject element not found in the pipeline"),
        Some(t_inject) => {
            t_inject.set_property(
                "tags",
                pipeless::gst::utils::tag_list_to_string(new_tag_list)
            )
        }
    };
}

fn create_processing_bin(stream: &StreamDef) -> gst::Bin {
    // The Pipeless output pipeline always receives RGB, so we only
    // worry about the output format
    let bin = gst::Bin::new();
    if stream.video.get_protocol() == "file" {
        if stream.video.get_location().ends_with(".mp4") {
            let videoconvert = pipeless::gst::utils::create_generic_component(
                "videoconvert", "videoconvert");
            let capsfilter = pipeless::gst::utils::create_generic_component(
                "capsfilter", "capsfilter");
            let encoder = pipeless::gst::utils::create_generic_component(
                "x264enc", "encoder");
            let taginject = pipeless::gst::utils::create_generic_component(
                "taginject", "taginject");
            let muxer = pipeless::gst::utils::create_generic_component(
                "mp4mux", "muxer");
            bin.add_many([
                &videoconvert, &capsfilter, &encoder, &taginject, &muxer
            ]).expect("Unable to add components to processing bin");

            let caps = gst::Caps::from_str("video/x-raw,format=I420")
                .expect("Unable to create caps from provided string");
            capsfilter.set_property("caps", caps);

            videoconvert.link(&capsfilter)
                .expect("Unable to link videoconvert to capsfilter");
            capsfilter.link(&encoder)
                .expect("Unable to link capsfilter to encoder");
            encoder.link(&taginject)
                .expect("Unable to link encoder to taginject");
            taginject.link(&muxer)
                .expect("Unable to link taginject to muxer");

            // Ghost pads to be able to plug other components to the bin
            let videoconvert_sink_pad = videoconvert.static_pad("sink")
                .expect("Failed to create the pipeline. Unable to get videoconvert sink pad.");
            let ghostpath_sink = gst::GhostPad::with_target(&videoconvert_sink_pad)
                .expect("Unable to create the sink ghost pad to link bin");
            bin.add_pad(&ghostpath_sink).expect("Unable to add sink pad");
            let muxer_src_pad = muxer.static_pad("src")
                .expect("Failed to create the pipeline. Unable to get muxer source pad.");
            let ghostpath_src = gst::GhostPad::with_target(&muxer_src_pad)
                .expect("Unable to create the ghost pad to link bin");
            bin.add_pad(&ghostpath_src).expect("Unable to add source pad");
        } else {
            panic!("Unsupported file type. Currently supported output extensions: .mp4");
        }
    } else if stream.video.get_protocol() == "rtmp" {
        let videoconvert = pipeless::gst::utils::create_generic_component(
            "videoconvert", "videoconvert");
        let queue = pipeless::gst::utils::create_generic_component(
            "queue", "queue");
        let encoder = pipeless::gst::utils::create_generic_component(
            "x264enc", "encoder");
        let taginject = pipeless::gst::utils::create_generic_component(
            "taginject", "taginject");
        let muxer = pipeless::gst::utils::create_generic_component(
            "flvmux", "muxer");
        bin.add_many([
            &videoconvert, &queue, &encoder, &taginject, &muxer
        ]).expect("Unable to add elements to processing bin");

        muxer.set_property("streamable", true);

        videoconvert.link(&queue)
            .expect("Unable to link videoconvert to queue");
        queue.link(&encoder)
            .expect("Unable to link queue to encoder");
        encoder.link(&taginject)
            .expect("Unable to link encoder to taginject");
        taginject.link(&muxer)
            .expect("Unable to link taginject to muxer");

        // Ghost pads to be able to plug other components to the bin
        let videoconvert_sink_pad = videoconvert.static_pad("sink")
            .expect("Failed to create the pipeline. Unable to get videoconvert sink pad.");
        let ghostpath_sink = gst::GhostPad::with_target(&videoconvert_sink_pad)
            .expect("Unable to create the sink ghost pad to link bin");
        bin.add_pad(&ghostpath_sink).expect("Unable to add ghostpad sink");
        let muxer_src_pad = muxer.static_pad("src")
            .expect("Failed to create the pipeline. Unable to get muxer source pad.");
        let ghostpath_src = gst::GhostPad::with_target(&muxer_src_pad)
            .expect("Unable to create the ghost pad to link bin");
        bin.add_pad(&ghostpath_src).expect("Unable to add ghostpad source");
    } else if stream.video.get_protocol() == "screen" {
        let queue1 = pipeless::gst::utils::create_generic_component(
            "queue", "queue1");
        let videoconvert = pipeless::gst::utils::create_generic_component(
            "videoconvert", "videoconvert");
        let queue2 = pipeless::gst::utils::create_generic_component(
            "queue", "queue2");
        bin.add_many([&queue1, &videoconvert, &queue2])
            .expect("Unable to add elements to processing bin");

        queue1.link(&videoconvert)
            .expect("Unable to link queue1 to videoconvert");
        videoconvert.link(&queue2)
            .expect("Unable to link videoconvert to queue2");

        // Ghost pads to be able to plug other components to the bin
        let queue1_sink_pad = queue1.static_pad("sink")
            .expect("Failed to create the pipeline. Unable to get queue1 sink pad.");
        let ghostpath_sink = gst::GhostPad::with_target(&queue1_sink_pad)
            .expect("Unable to create the sink ghost pad to link bin");
        bin.add_pad(&ghostpath_sink).expect("Unable to add ghost pad to processing bin");
        let queue2_src_pad = queue2.static_pad("src")
            .expect("Failed to create the pipeline. Unable to get queue2 source pad.");
        let ghostpath_src = gst::GhostPad::with_target(&queue2_src_pad)
            .expect("Unable to create the ghost pad to link bin");
        bin.add_pad(&ghostpath_src).expect("Unable to add ghost pad to processing bin");
    } else {
        panic!("Unsupported output protocol. Please contact us if you think a new protocol is needed or feel free send us a GitHub PR adding it");
    }

    bin
}

fn get_sink(sink_type: &str, location: Option<&str>) -> gst::Element {
    let sink = pipeless::gst::utils::create_generic_component(
        sink_type, "sink");
    if let Some(l) = location {
        sink.set_property("location", l);
    }

    sink
}

fn create_sink(stream: &StreamDef) -> gst::Element {
    let location = stream.video.get_location();
    return match stream.video.get_protocol() {
        // TODO: implement processing bin for all the below protocols
        "file" => get_sink("filesink", Some(location)),
        "https" => get_sink("souphttpsink", Some(location)),
        "rtmp" => get_sink("rtmpsink", Some(format!("rtmp://{}", location).as_ref())),
        "rstp" => get_sink("rtspclientsink", Some(location)),
        "screen" => get_sink("autovideosink", None),
        _ => {
            warn!("unsupported output protocol, defaulting to screen");
            get_sink("autovideosink", None)
        }
    };
}

fn on_bus_message(
    msg: &gst::Message,
    pipeline_id: uuid::Uuid,
    pipeless_bus_sender: &tokio::sync::mpsc::UnboundedSender<pipeless::events::Event>,
) {
    match msg.view() {
        gst::MessageView::Eos(_eos) => {
            info!("End of stream in output gst pipeline. Pipeline id: {}", pipeline_id);

            pipeless::events::publish_ouptut_eos_event_sync(pipeless_bus_sender);
        },
        gst::MessageView::Error(err) => {
            let err_msg = err.error().message().to_string();
            let debug_msg = match err.debug() {
                Some(m) => m.as_str().to_string(),
                None => "".to_string()
            };
            let err_src_name = match err.src() {
                Some(src) => src.name(),
                None => "no_name".into()
            };
            debug!("Debug info for the following error: {}", debug_msg);
            // Communicate error
            pipeless::events::publish_output_stream_error_event_sync(pipeless_bus_sender, &err_msg);
            // Exit the the output thread with the error. This will stop the mainloop.
            panic!(
                "Error in output gst pipeline from element {}.
                Pipeline id: {}. Error: {}",
                err_src_name, pipeline_id, err_msg
            );
        },
        gst::MessageView::Warning(w) => {
            let warn_msg = w.error().message().to_string();
            let debug_msg = match w.debug() {
               Some(m) => m.as_str().to_string(),
               None => "".to_string()
            };
            let msg_src = match msg.src() {
                Some(src) => src.name(),
                None => "Element Not Obtained".into()
            };
            warn!(
                "Warning in output gst pipeline from element element {}.
                Pipeline id: {}. Warning: {}",
                msg_src, pipeline_id, warn_msg
            );
            debug!("Debug info: {}", debug_msg);
        },
        gst::MessageView::StateChanged(sts) => {
            let old_state = pipeless::gst::utils::format_state(sts.old());
            let current_state =
                pipeless::gst::utils::format_state(sts.current());
            let pending_state =
                pipeless::gst::utils::format_state(sts.pending());
            debug!(
                "Output gst pipeline state change. Pipeline id: {}.
                Old state: {}. Current state: {}. Pending state: {}",
                pipeline_id, old_state, current_state, pending_state);
        },
        _ => debug!("Unhandled message on output gst pipeline bus.
                Pipeline id: {}", pipeline_id)
    }
}

fn create_gst_pipeline(
    output_stream_def: &StreamDef,
    caps: &str
) -> (gst::Pipeline, gst::BufferPool) {
    let pipeline = gst::Pipeline::new();
    let input_stream_caps = gst::Caps::from_str(caps).expect(format!(
        "Unable to create caps from provide string {}", caps).as_ref());
    let caps_structure = input_stream_caps.structure(0)
        .expect("Unable to get structure from capabilities");
    let caps_width = pipeless::gst::utils::i32_from_caps_structure(
        &caps_structure, "width"
    ).expect("Unable to get width from original input caps") as u32;
    let caps_height = pipeless::gst::utils::i32_from_caps_structure(
        &caps_structure, "height"
    ).expect("Unable to get height from original input caps") as u32;
    let caps_framerate_fraction =
        pipeless::gst::utils::fraction_from_caps_structure(
            &caps_structure, "framerate"
        ).expect("Unable to get framerate from original input caps to create output");
    // The appsrc caps will be the caps from the input stream but in RGB format (produced by the workers)
    let appsrc_caps_str = format!(
        "video/x-raw,format=RGB,width={},height={},framerate={}/{}",
        caps_width, caps_height,
        caps_framerate_fraction.0, caps_framerate_fraction.1
    );
    let appsrc_caps =
        gst::Caps::from_str(&appsrc_caps_str).expect(format!(
            "Unable to create appsrc caps from {}",
            appsrc_caps_str).as_ref()
        );

    let appsrc = gst::ElementFactory::make("appsrc")
        .name("appsrc")
        .property("is-live", true)
        .property("do-timestamp", false)
        .property("format", gst::Format::Time)
        .property("max-bytes", 500000000 as u64) // Queue size
        .property("caps", &appsrc_caps)
        .build()
        .expect("Failed to create appsrc");

    let processing_bin = create_processing_bin(output_stream_def);
    let sink = create_sink(output_stream_def);

    pipeline.add_many([&appsrc, &sink])
        .expect("Unable to add elements to the pipeline");
    pipeline.add(&processing_bin)
        .expect("Unable to add processing bin to the pipeline");
    appsrc.link(&processing_bin)
        .expect("Unable to link appsrc to processing bin");
    processing_bin.link(&sink)
        .expect("Unable to link processing bin to sink");

    // The tags can be sent by the input before the output
    // pipeline is created
    match &output_stream_def.initial_tags {
        Some(tag_list) => set_pipeline_tags(&pipeline, &tag_list),
        None => ()
    }

    // Used to remove overhead of creating buffers all the time
    // when sending frames
    let bufferpool = gst::BufferPool::new();
    let mut bufferpool_config = bufferpool.config();
    let frame_size = caps_width * caps_height * 3;
	bufferpool_config.set_params(Some(&appsrc_caps), frame_size,0, 0);
    bufferpool.set_config(bufferpool_config).expect("Unable to set bufferpool config");
	bufferpool.set_active(true).expect("Could not activate buffer pool");

    (pipeline, bufferpool)
}

pub struct Pipeline {
    id: uuid::Uuid, // Id of the parent pipeline (the one that groups input and output)
    // Since the output is not mandatory there could be
    // no gst output pipeline associated with the Pipeless output.
    gst_pipeline: gst::Pipeline,
    stream: pipeless::output::pipeline::StreamDef,
    buffer_pool: gst::BufferPool, // Allows to send buffers without constantly allocate new ones
}
impl Pipeline {
    pub fn new(
        id: uuid::Uuid,
        stream: pipeless::output::pipeline::StreamDef,
        caps: &str,
        pipeless_bus_sender: &tokio::sync::mpsc::UnboundedSender<pipeless::events::Event>,
    ) -> Self {
        let (gst_pipeline, buffer_pool) = create_gst_pipeline(&stream, caps);
        let pipeline = Pipeline {
            id,
            gst_pipeline,
            stream,
            buffer_pool,
        };
        let bus = pipeline.gst_pipeline.bus()
            .expect("Unable to get output gst pipeline bus");
        bus.add_signal_watch();
        let pipeline_id = pipeline.id.clone();
        bus.connect_message(
            None, // None avoids to filter by msg type
            {
                let pipeless_bus_sender = pipeless_bus_sender.clone();
                move |_bus, msg| {
                    on_bus_message(&msg, pipeline_id, &pipeless_bus_sender);
                }
            }
        );
        pipeline.gst_pipeline
            .set_state(gst::State::Playing)
            .expect("Unable to start the output gst pipeline");

        pipeline
    }

    pub fn get_pipeline_id(&self) -> uuid::Uuid {
        self.id
    }

    pub fn close(&self) -> Result<gst::StateChangeSuccess, gst::StateChangeError>{
        self.gst_pipeline.set_state(gst::State::Null)
    }

    /// Invoked by the pipeline manager when there is an EOS
    /// event on the input
    pub fn on_eos(&self) {
        let appsrc = self.gst_pipeline.by_name("appsrc")
            .expect("Unable to get pipeline appsrc element")
            .dynamic_cast::<gst_app::AppSrc>()
            .expect("Unable to cast element to AppSource");
        appsrc.end_of_stream()
            .expect("Error sending EOS signal to output");
    }

    pub fn on_new_frame(&self, frame: pipeless::data::Frame) {
        match frame {
            pipeless::data::Frame::RgbFrame(rgb_frame) => {
                let modified_pixels = rgb_frame.get_modified_pixels();
                let out_frame_data = modified_pixels.as_slice()
                    .expect("Unable to get bytes data from RGB frame. Is your output image of the same shape as the input?");

                let appsrc = self.gst_pipeline.by_name("appsrc")
                    .expect("Unable to get pipeline appsrc element")
                    .dynamic_cast::<gst_app::AppSrc>()
                    .expect("Unable to cast element to AppSource");
                let copy_timestamps =
                    self.stream.get_video().get_protocol() != "screen";

                // TODO: something we can do instead of having a buffer pool is to re-use the input gst buffer by storing it into the RgbFrame
                let mut gst_buffer = self.buffer_pool.acquire_buffer(None)
                    .expect("Unable to acquire buffer from pool");

                let gst_buffer_mut = gst_buffer.get_mut().expect("Unable to get mutable buffer");

                // TODO: profile. Could this be faster by copying manually with rayon?
                // let data_slice = buffer_map.as_mut_slice();
                //// Use Rayon to assign data elements in parallel
                // data_slice.par_iter_mut().for_each(|byte| {
                //     *byte = (*byte + 1) % 256;
                // });
                gst_buffer_mut.copy_from_slice(0, out_frame_data)
                    .expect("Unable to copy slice into buffer");

                if copy_timestamps {
                    let pts = rgb_frame.get_pts();
                    let dts = rgb_frame.get_dts();
                    let duration = rgb_frame.get_duration();
                    gst_buffer_mut.set_pts(pts);
                    gst_buffer_mut.set_dts(dts);
                    gst_buffer_mut.set_duration(duration);
                }

                if let Err(err) = appsrc.push_buffer(gst_buffer) {
                    error!("Failed to send the output buffer: {}", err);
                }
            }
        }
    }

    pub fn on_new_tags(&self, new_tags: gst::TagList) {
        let merged_tags = new_tags;
        match self.gst_pipeline.by_name("taginject") {
            None => warn!("Taginject element not found, skipping tags update."),
            Some(_t_inject) => {
                // FIXME: Gstreamer bug taginject tags are not readable when they should.
                //        Uncomment the following 2 lines when fixed to update tags properly.
                //       Ref: https://gitlab.freedesktop.org/gstreamer/gstreamer/-/issues/2889
                //let current_tags = t_inject.property::<gst::TagList>("tags");
                //merged_tags = current_tags.merge(&merged_tags, gst::TagMergeMode::Append);
            }
        };
        set_pipeline_tags(&self.gst_pipeline, &merged_tags);
    }
}