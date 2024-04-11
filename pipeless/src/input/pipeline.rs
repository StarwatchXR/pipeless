use glib::BoolError;
use log::{error, info, warn, debug};
use std;
use std::str::FromStr;
use gstreamer as gst;
use gst::prelude::*;
use gstreamer_app as gst_app;
use uuid;
use ndarray;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;

use crate as pipeless;

#[derive(Debug)]
pub struct InputPipelineError {
    msg: String,
}

type DecibelSharedState = Arc<Mutex<Option<f64>>>;

impl InputPipelineError {
    fn new(msg: &str) -> Self {
        Self { msg: msg.to_owned() }
    }
}

impl std::error::Error for InputPipelineError {}

impl std::fmt::Display for InputPipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg.to_string())
    }
}

impl From<BoolError> for InputPipelineError {
    fn from(error: BoolError) -> Self {
        Self {
            msg: error.to_string(),
        }
    }
}

impl From<pipeless::config::video::VideoConfigError> for InputPipelineError {
    fn from(error: pipeless::config::video::VideoConfigError) -> Self {
        Self {
            msg: error.to_string(),
        }
    }
}

/// Each Pipeline contains a single Stream (which could have audio + video + subtitles, etc)
/// This struct defines a Stream. You can think on it like a stream configuration
#[derive(Clone)]
pub struct StreamDef {
    video: pipeless::config::video::Video,
}

impl StreamDef {
    pub fn new(uri: String) -> Result<Self, InputPipelineError> {
        let video = pipeless::config::video::Video::new(uri)?;
        Ok(Self { video })
    }

    pub fn get_video(&self) -> &pipeless::config::video::Video {
        &self.video
    }
}

fn on_new_sample(
    pipeless_pipeline_id: uuid::Uuid,
    appsink: &gst_app::AppSink,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
    frame_number: &mut u64,
    decibel_shared_state: &Arc<Mutex<Option<f64>>>,
    uri: String,
) -> Result<gst::FlowSuccess, gst::FlowError> {
    info!("New Sample");
    let sample = appsink.pull_sample().map_err(|_err| {
        error!("Sample is None");
        gst::FlowError::Error
    })?;

    let buffer = sample.buffer().ok_or_else(|| {
        error!("The sample buffer is None");
        gst::FlowError::Error
    })?;

    let frame_input_instant = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
    let caps = sample.caps().ok_or_else(|| {
        error!("Unable to get sample capabilities");
        gst::FlowError::Error
    })?;
    let caps_structure = caps.structure(0).ok_or_else(|| {
        error!("Unable to get structure from capabilities");
        gst::FlowError::Error
    })?;
    let width = pipeless::gst::utils::i32_from_caps_structure(
        &caps_structure, "width",
    )? as usize; // We need to cast to usize for the ndarray dimension
    let height = pipeless::gst::utils::i32_from_caps_structure(
        &caps_structure, "height",
    )? as usize; // We need to cast to usize for the ndarray dimension
    let channels: usize = 3; // RGB
    let framerate_fraction = pipeless::gst::utils::fraction_from_caps_structure(
        &caps_structure, "framerate",
    )?;
    let fps = framerate_fraction.0 / framerate_fraction.1;
    let pts = buffer.pts().ok_or_else(|| {
        error!("Unable to get presentation timestamp");
        gst::FlowError::Error
    })?;
    let dts = match buffer.dts() {
        Some(d) => d,
        None => {
            debug!("Decoding timestamp not present on frame");
            gst::ClockTime::ZERO
        }
    };
    let duration = buffer.duration().or(Some(gst::ClockTime::from_mseconds(0))).unwrap();
    let buffer_info = buffer.map_readable().or_else(|_| {
        error!("Unable to extract the info from the sample buffer.");
        Err(gst::FlowError::Error)
    })?;

    let ndframe = ndarray::Array3::from_shape_vec(
        (height, width, channels), buffer_info.to_vec(),
    ).map_err(|err| {
        error!("Failed to create ndarray from buffer data: {}", err.to_string());
        gst::FlowError::Error
    })?;

    *frame_number += 1;
    // Lock the shared state and read the current decibel value
    let decibel_lock = decibel_shared_state.lock().unwrap();
    let current_decibel_value = (*decibel_lock).unwrap_or(0.0); // Defaulting to 0.0 if None

    let frame = pipeless::data::Frame::new_rgb(
        ndframe, width, height,
        pts, dts, duration,
        fps as u8, frame_input_instant,
        pipeless_pipeline_id, *frame_number,
        current_decibel_value, uri,
    );
    // The event takes ownership of the frame
    pipeless::events::publish_new_frame_change_event_sync(
        pipeless_bus_sender, frame,
    );

    Ok(gst::FlowSuccess::Ok)
}

fn on_new_audio_sample(
    pipeless_pipeline_id: uuid::Uuid,
    appsink: &gst_app::AppSink,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
    decibel_shared_state: &Arc<Mutex<Option<f64>>>,
) -> Result<gst::FlowSuccess, gst::FlowError> {
    let sample = appsink.pull_sample().map_err(|_err| {
        gst::element_error!(appsink, gst::CoreError::Failed, ("Failed to pull sample"));
        gst::FlowError::Error
    })?;

    let buffer = sample.buffer().ok_or_else(|| {
        gst::element_error!(appsink, gst::CoreError::Failed, ("Sample buffer is None"));
        gst::FlowError::Error
    })?;

    let caps = sample.caps().ok_or_else(|| {
        gst::element_error!(appsink, gst::CoreError::Failed, ("Sample caps are None"));
        gst::FlowError::Error
    })?;

    let structure = caps.structure(0).ok_or_else(|| {
        gst::element_error!(appsink, gst::CoreError::Failed, ("Caps structure is None"));
        gst::FlowError::Error
    })?;

    // Extract format information from the caps structure
    let format_type = structure.name();
    let channels = structure.get::<i32>("channels").unwrap_or(1) as usize; // Default to mono if not specified

    // Map the buffer to access data
    let map_info = buffer.map_readable().map_err(|_| {
        gst::element_error!(appsink, gst::CoreError::Failed, ("Could not map buffer"));
        gst::FlowError::Error
    })?;

    let format_type = structure.get::<&str>("format").expect("Failed to get format type");

    // Handle different audio formats
    let audio_data = match format_type {
        "S16LE" => {
            // Convert bytes to i16 samples and then to f64 for processing
            let samples_slice = map_info.as_slice();
            samples_slice
                .chunks_exact(2)
                .map(|c| i16::from_ne_bytes(c.try_into().unwrap()) as f64)
                .collect::<Vec<_>>()
        }
        "F32LE" => {
            // Convert bytes to f32 samples and then to f64 for processing
            let samples = map_info.as_slice().chunks_exact(4).map(|c| f32::from_ne_bytes(c.try_into().unwrap()) as f64).collect::<Vec<_>>();
            samples
        }
        _ => {
            eprintln!("Unsupported audio format: {}", format_type);
            return Err(gst::FlowError::Error);
        }
    };

    // For stereo (or more channels), average the channels to mono
    let mono_audio_data = if channels > 1 {
        audio_data.chunks_exact(channels).map(|chunk| chunk.iter().sum::<f64>() / channels as f64).collect::<Vec<f64>>()
    } else {
        audio_data
    };

    let rms = calculate_rms(&mono_audio_data);
    let decibels = calculate_decibels(rms);

    // Lock the shared state and update it with the new decibel value
    let mut decibel_lock = decibel_shared_state.lock().unwrap();
    *decibel_lock = Some(decibels);

    Ok(gst::FlowSuccess::Ok)
}

fn calculate_rms(audio_data: &[f64]) -> f64 {
    let sum_of_squares: f64 = audio_data.iter().map(|&sample| sample.powi(2)).sum();
    (sum_of_squares / audio_data.len() as f64).sqrt()
}

fn calculate_decibels(rms: f64) -> f64 {
    20.0 * rms.log10()
}


fn on_pad_added(
    pad: &gst::Pad,
    _info: &mut gst::PadProbeInfo,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
) -> gst::PadProbeReturn {
    let caps = match pad.current_caps() {
        Some(c) => c,
        None => {
            warn!("Could not get caps from a new added pad");
            return gst::PadProbeReturn::Ok; // Leave the probe in place
        }
    };
    info!("Dynamic source pad {} caps: {}",
        pad.name().as_str(), caps.to_string());

    pipeless::events::publish_new_input_caps_event_sync(
        pipeless_bus_sender, caps.to_string(),
    );

    // The probe is no more needed since we already got the caps
    return gst::PadProbeReturn::Remove;
}

fn create_video_processing_bin(
    pipeless_pipeline_id: uuid::Uuid,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
    shared_decibel_state: &Arc<Mutex<Option<f64>>>,
    uri: &str,
) -> Result<gst::Bin, InputPipelineError> {
    info!("Creating video processing bin");

    // Config
    let processing_frame_rate = 1;

    let bin = gst::Bin::new();
    let videoconvert = pipeless::gst::utils::create_generic_component("videoconvert", "videoconvert")?;
    // Only used when in Nvidia devices
    let nvvidconv_opt = pipeless::gst::utils::create_generic_component("nvvideoconvert", "nvvidconv");

    let videorate = pipeless::gst::utils::create_generic_component("videorate", "videorate")
        .map_err(|_| InputPipelineError::new("Failed to create videorate element"))?;

    let caps = gst::Caps::builder("video/x-raw")
        .field("framerate", &gst::Fraction::new(1, 3))
        .build();

    let capsfilter = pipeless::gst::utils::create_generic_component("capsfilter", "capsfilter")
        .map_err(|_| InputPipelineError::new("Failed to create capsfilter"))?;
    capsfilter.set_property("caps", &caps);


    // Force RGB output since workers process RGB
    let sink_caps = gst::Caps::from_str("video/x-raw,format=RGB")
        .map_err(|_| { InputPipelineError::new("Unable to create caps from string") })?;

    let appsink = gst::ElementFactory::make("appsink")
        .name("appsink")
        .property("emit-signals", true)
        .property("caps", sink_caps)
        .build()
        .map_err(|_| { InputPipelineError::new("Failed to create appsink") })?
        .dynamic_cast::<gst_app::AppSink>()
        .map_err(|_| { InputPipelineError::new("Unable to cast element to AppSink") })?;

    let shared_decibel_state_clone = Arc::clone(&shared_decibel_state);
    let uri_owned = uri.to_owned();

    let appsink_callbacks = gst_app::AppSinkCallbacks::builder()
        .new_sample(
            {
                let pipeless_bus_sender = pipeless_bus_sender.clone();
                let mut frame_number: u64 = 0; // Used to set the frame number
                move |appsink: &gst_app::AppSink| {
                    let uri_clone = uri_owned.clone();
                    on_new_sample(
                        pipeless_pipeline_id,
                        appsink,
                        &pipeless_bus_sender,
                        &mut frame_number,
                        &shared_decibel_state_clone,
                        uri_clone,
                    )
                }
            }).build();
    appsink.set_callbacks(appsink_callbacks);

    let appsink_element = appsink.upcast_ref::<gst::Element>();

    bin.add_many([&videoconvert, &videorate, &capsfilter, &appsink_element])
        .map_err(|_| { InputPipelineError::new("Unable to add elements to the input bin") })?;


    if let Ok(nvvidconv) = &nvvidconv_opt {
        info!("Adding nvvideoconvert");
        bin.add(nvvidconv)
            .map_err(|_| { InputPipelineError::new("Unable to add nvidconv to the input bin") })?;
        nvvidconv.link(&videoconvert) // We use unwrap here because it cannot be none
            .map_err(|_| { InputPipelineError::new("Error linking nvvidconv to videoconvert") })?;

        let nvvidconv_sink_pad = nvvidconv.static_pad("sink")
            .ok_or_else(|| { InputPipelineError::new("Unable to get nvvidconv pad") })?;


        let ghostpath_sink = gst::GhostPad::with_target(&nvvidconv_sink_pad)
            .map_err(|_| { InputPipelineError::new("Unable to create the ghost pad to link bin") })?;

        bin.add_pad(&ghostpath_sink)
            .map_err(|_| { InputPipelineError::new("Unable to add ghostpad to input bin") })?;
    } else {
        info!("Skipping nvidia decoder");
        // Create ghost pad to be able to plug other components
        let videoconvert_sink_pad = match videoconvert.static_pad("sink") {
            Some(pad) => pad,
            None => {
                return Err(InputPipelineError::new("Failed to create the pipeline. Unable to get videoconvert source pad."));
            }
        };

        let ghostpath_sink = gst::GhostPad::with_target(&videoconvert_sink_pad)
            .map_err(|_| { InputPipelineError::new("Unable to create the ghost pad to link bin") })?;

        bin.add_pad(&ghostpath_sink)
            .map_err(|_| { InputPipelineError::new("Unable to add ghostpad to input bin") })?;
    }


    videoconvert.link(&videorate).map_err(|_| InputPipelineError::new("Error linking video convert to video rate"))?;
    videorate.link(&capsfilter).map_err(|_| InputPipelineError::new("Error linking video rate to caps filter"))?;
    capsfilter.link(&appsink).map_err(|_| InputPipelineError::new("Error linking Caps filter to App sink"))?;

    return Ok(bin);
}

fn create_audio_processing_bin(
    pipeless_pipeline_id: uuid::Uuid,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
    shared_decibel_state: &Arc<Mutex<Option<f64>>>,
) -> Result<gst::Bin, InputPipelineError> {
    info!("Creating audio processing bin");
    let bin = gst::Bin::new();
    let audioconvert = pipeless::gst::utils::create_generic_component("audioconvert", "audioconvert")?;
    let level = pipeless::gst::utils::create_generic_component("level", "level")?;

    // Create ghost pad to be able to plug other components
    let audioconvert_sink_pad = match audioconvert.static_pad("sink") {
        Some(pad) => pad,
        None => {
            return Err(InputPipelineError::new("Failed to create the pipeline. Unable to get audioconvert source pad."));
        }
    };


    let audiosink = gst::ElementFactory::make("appsink")
        .name("appsink")
        .property("emit-signals", true)
        // .property("caps", audio_sink_caps)
        .build()
        .map_err(|_| { InputPipelineError::new("Failed to create appsink") })?
        .dynamic_cast::<gst_app::AppSink>()
        .map_err(|_| { InputPipelineError::new("Unable to cast element to AppSink") })?;

    let shared_decibel_state_clone = Arc::clone(&shared_decibel_state);

    let audio_appsink_callbacks = gst_app::AppSinkCallbacks::builder()
        .new_sample({
            let pipeless_bus_sender = pipeless_bus_sender.clone();
            move |appsink: &gst_app::AppSink| {
                on_new_audio_sample(
                    pipeless_pipeline_id,
                    appsink,
                    &pipeless_bus_sender,
                    &shared_decibel_state_clone,
                )
            }
        })
        .build();

    audiosink.set_callbacks(audio_appsink_callbacks);


    let audiosink_element = audiosink.upcast_ref::<gst::Element>();

    bin.add_many([&audioconvert, &level, audiosink_element])
        .map_err(|_| { InputPipelineError::new("Unable to add elements to the audio bin") })?;

    let ghostpath_sink = gst::GhostPad::with_target(&audioconvert_sink_pad)
        .map_err(|_| { InputPipelineError::new("Unable to create the ghost pad to link bin") })?;

    bin.add_pad(&ghostpath_sink)
        .map_err(|_| { InputPipelineError::new("Unable to add ghostpad to input bin") })?;


    audioconvert.link(&level).map_err(|_| InputPipelineError::new("Error linking audio convert to level"))?;
    level.link(&audiosink).map_err(|_| InputPipelineError::new("Error linking audio level to app sink"))?;

    return Ok(bin);
}

fn on_bus_message(
    msg: &gst::Message,
    pipeline_id: uuid::Uuid,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
) {
    match msg.view() {
        gst::MessageView::Eos(eos) => {
            let eos_src_name = match eos.src() {
                Some(src) => src.name(),
                None => "no_name".into()
            };

            info!("Received received EOS from source {}.
                Pipeline id: {} ended", eos_src_name, pipeline_id);

            pipeless::events::publish_input_eos_event_sync(pipeless_bus_sender);
        }
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
            pipeless::events::publish_input_stream_error_event_sync(pipeless_bus_sender, &err_msg);
            // Exit thread, thus glib pipeline mainloop.
            error!(
                "Error in input gst pipeline from element {}.
                Pipeline id: {}. Error: {}",
                err_src_name, pipeline_id, err_msg
            );
        }
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
                "Warning received in input gst pipeline from element {}.
                Pipeline id: {}. Warning: {}",
                msg_src, pipeline_id, warn_msg);
            debug!("Debug info: {}", debug_msg);
        }
        gst::MessageView::StateChanged(sts) => {
            let old_state = pipeless::gst::utils::format_state(sts.old());
            let current_state = pipeless::gst::utils::format_state(sts.current());
            let pending_state = pipeless::gst::utils::format_state(sts.pending());
            debug!(
                "Input gst pipeline state change. Pipeline id: {}.
                Old state: {}. Current state: {}. Pending state: {}",
                pipeline_id, old_state, current_state, pending_state);
        }
        gst::MessageView::Tag(tag) => {
            let tags = tag.tags();
            info!(
                "New tags for input gst pipeline with id {}. Tags: {}",
                pipeline_id, tags);

            pipeless::events::publish_input_tags_changed_event_sync(
                pipeless_bus_sender, tags,
            );
        }
        _ => debug!("
            Unhandled message on input gst pipeline bus.
            Pipeline id: {}", pipeline_id)
    }
}


fn create_gst_pipeline(
    pipeless_pipeline_id: uuid::Uuid,
    input_uri: &str,
    pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
    shared_decibel_sender: &Arc<Mutex<Option<f64>>>,
) -> Result<gst::Pipeline, InputPipelineError> {
    let pipeline = gst::Pipeline::new();

    let uridecodebin = pipeless::gst::utils::create_generic_component("uridecodebin3", "source")?;
    let videoqueue = pipeless::gst::utils::create_generic_component("queue", "videoqueue")?;
    let audioqueue = pipeless::gst::utils::create_generic_component("queue", "audioqueue")?;

    let audiobin = create_audio_processing_bin(
        pipeless_pipeline_id, &pipeless_bus_sender, &shared_decibel_sender,
    )?;
    let videobin = create_video_processing_bin(
        pipeless_pipeline_id, &pipeless_bus_sender, &shared_decibel_sender, input_uri,
    )?;

// Convert `Bin` to `Element` using `upcast` method
    let audiobin_element = audiobin.upcast::<gst::Element>();
    let videobin_element = videobin.upcast::<gst::Element>();

    pipeline.add_many([&uridecodebin, &videoqueue, &audioqueue, &audiobin_element, &videobin_element])
        .map_err(|_| { InputPipelineError::new("Unable to add queue elements to the input bin") })?;


    uridecodebin.set_property("uri", input_uri);

    audioqueue.link(&audiobin_element)
        .map_err(|_| { InputPipelineError::new("Unable to link audio queue to audio processing") })?;

    videoqueue.link(&videobin_element)
        .map_err(|_| { InputPipelineError::new("Unable to link video queue to video processing") })?;

    let link_new_pad_fn = move |pad: &gst::Pad| -> Result<gst::PadLinkSuccess, InputPipelineError> {
        let pad_caps = pad.query_caps(None);
        if let Some(structure) = pad_caps.structure(0) {
            let structure_name = structure.name();
            info!("Structure : {}", structure_name);
            // Check if the structure name indicates this is an audio pad
            if structure_name.starts_with("audio/") {
                let audioqueue_sink_pad = match audioqueue.static_pad("sink") {
                    Some(pad) => pad,
                    None => {
                        return Err(InputPipelineError::new("Failed to create the pipeline. Unable to get audioconvert source pad."));
                    }
                };


                pad.link(&audioqueue_sink_pad)
                    .map_err(|_| { InputPipelineError::new("Unable to link new uridecodebin pad to audio sink pad") }).expect("TODO: panic message");
                Ok(gst::PadLinkSuccess)
            } else if structure_name.starts_with("video/") {
                let videoqueue_sink_pad = match videoqueue.static_pad("sink") {
                    Some(pad) => pad,
                    None => {
                        return Err(InputPipelineError::new("Failed to create the pipeline. Unable to get videoconvert source pad."));
                    }
                };

                pad.link(&videoqueue_sink_pad)
                    .map_err(|_| { InputPipelineError::new("Unable to link new uridecodebin pad to video sink pad") }).expect("TODO: panic message");
                Ok(gst::PadLinkSuccess)
            } else {
                return Err(InputPipelineError::new("Failed to get structure from pad's caps"));
            }
        } else {
            return Err(InputPipelineError::new("Failed to get structure from pad's caps"));
        }
    };

    uridecodebin.connect_pad_added({
        let pipeless_bus_sender = pipeless_bus_sender.clone();
        move |_elem, pad| {
            let link_pad_res = link_new_pad_fn(&pad);
            match link_pad_res {
                Ok(_) => {
                    // Connect an async handler to the pad to be notified when caps are set
                    pad.add_probe(
                        gst::PadProbeType::EVENT_UPSTREAM,
                        {
                            let pipeless_bus_sender = pipeless_bus_sender.clone();
                            move |pad: &gst::Pad, info: &mut gst::PadProbeInfo| {
                                on_pad_added(pad, info, &pipeless_bus_sender)
                            }
                        },
                    );
                }
                Err(err) => error!("{}", err)
            }
        }
    });


    Ok(pipeline)
}

pub struct Pipeline {
    id: uuid::Uuid,
    // Id of the parent pipeline (the one that groups input and output)
    _stream: pipeless::input::pipeline::StreamDef,
    gst_pipeline: gst::Pipeline,
}

impl Pipeline {
    pub fn new(
        id: uuid::Uuid,
        stream: pipeless::input::pipeline::StreamDef,
        pipeless_bus_sender: &tokio::sync::mpsc::Sender<pipeless::events::Event>,
    ) -> Result<Self, InputPipelineError> {
        let input_uri = stream.get_video().get_uri();

        // Exchange data between video and audio processing
        let decibel_shared_state = Arc::new(Mutex::new(None));

        let gst_pipeline = create_gst_pipeline(id, input_uri, pipeless_bus_sender, &decibel_shared_state)?;
        let pipeline = Pipeline {
            id,
            _stream: stream,
            gst_pipeline,
        };

        let bus = pipeline.gst_pipeline.bus()
            .ok_or_else(|| { InputPipelineError::new("Unable to get input gst pipeline bus") })?;
        bus.add_signal_watch();
        let pipeline_id = pipeline.id.clone();
        bus.connect_message(
            None,
            {
                let pipeless_bus_sender = pipeless_bus_sender.clone();
                move |_bus, msg| {
                    on_bus_message(&msg, pipeline_id, &pipeless_bus_sender);
                }
            },
        );

        pipeline.gst_pipeline
            .set_state(gst::State::Playing)
            .map_err(|_| { InputPipelineError::new("Unable to start the input gst pipeline") })?;

        Ok(pipeline)
    }

    pub fn get_pipeline_id(&self) -> uuid::Uuid {
        self.id
    }

    pub fn close(&self) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        self.gst_pipeline.set_state(gst::State::Null)
    }
}
