use std::{self, str::FromStr};
use ndarray;
use uuid;
use gstreamer as gst;

/// Custom data that the user can add to the frame in a stage
/// allowing to pass data to subsequent stages
pub enum UserData {
    Empty,
    Integer(i32),
    Float(f64),
    String(String),
    Array(Vec<UserData>),
    Dictionary(Vec<(String, UserData)>),
}

pub enum InferenceOutput {
    Default(ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>>),
    OnnxInferenceOutput(crate::stages::inference::onnx::OnnxInferenceOutput),
}

pub struct RgbFrame {
    uuid: uuid::Uuid,
    original: ndarray::Array3<u8>,
    modified: ndarray::Array3<u8>,
    width: usize,
    height: usize,
    pts: gst::ClockTime,
    dts: gst::ClockTime,
    duration: gst::ClockTime,
    fps: u8,
    input_ts: f64,
    // epoch in seconds
    inference_input: ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>>,
    // We can convert the output into an arrayview since the user does not need to modify it and the inference runtimes returns a view, so we avoid a copy
    inference_output: InferenceOutput,
    pipeline_id: uuid::Uuid,
    user_data: UserData,
    frame_number: u64,
    decibel_level: f64,
}

impl RgbFrame {
    pub fn new(
        original: ndarray::Array3<u8>,
        width: usize, height: usize,
        pts: gst::ClockTime, dts: gst::ClockTime, duration: gst::ClockTime,
        fps: u8, input_ts: f64,
        pipeline_id: uuid::Uuid, frame_number: u64, decibel_level: f64,
    ) -> Self {
        let modified = original.to_owned();
        RgbFrame {
            uuid: uuid::Uuid::new_v4(),
            original,
            modified,
            width,
            height,
            pts,
            dts,
            duration,
            fps,
            input_ts,
            inference_input: ndarray::ArrayBase::zeros(ndarray::IxDyn(&[0])),
            inference_output: InferenceOutput::Default(ndarray::ArrayBase::zeros(ndarray::IxDyn(&[0]))),
            pipeline_id,
            user_data: UserData::Empty,
            frame_number,
            decibel_level,
        }
    }

    pub fn from_values(
        uuid: &str,
        original: ndarray::Array3<u8>,
        modified: ndarray::Array3<u8>,
        width: usize, height: usize,
        pts: u64, dts: u64, duration: u64,
        fps: u8, input_ts: f64,
        inference_input: ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>>,
        inference_output: InferenceOutput,
        pipeline_id: &str,
        user_data: UserData, frame_number: u64, decibel_level: f64
    ) -> Self {
        RgbFrame {
            uuid: uuid::Uuid::from_str(uuid).unwrap(),
            original,
            modified,
            width,
            height,
            pts: gst::ClockTime::from_mseconds(pts),
            dts: gst::ClockTime::from_mseconds(dts),
            duration: gst::ClockTime::from_mseconds(duration),
            fps,
            input_ts,
            inference_input,
            inference_output,
            pipeline_id: uuid::Uuid::from_str(pipeline_id).unwrap(),
            user_data: user_data,
            frame_number,
            decibel_level,
        }
    }

    pub fn set_original_pixels(&mut self, original_pixels: ndarray::Array3<u8>) {
        self.original = original_pixels
    }
    pub fn get_original_pixels(&self) -> ndarray::ArrayView3<u8> {
        self.original.view()
    }
    pub fn get_modified_pixels(&mut self) -> ndarray::ArrayViewMut3<u8> {
        self.modified.view_mut()
    }
    pub fn update_mutable_pixels(
        &mut self, view_mut: ndarray::ArrayViewMut3<u8>,
    ) {
        self.modified.assign(&view_mut);
    }
    pub fn get_uuid(&self) -> uuid::Uuid {
        self.uuid
    }
    pub fn get_fps(&self) -> u8 {
        self.fps
    }
    pub fn get_pts(&self) -> gst::ClockTime {
        self.pts
    }
    pub fn get_dts(&self) -> gst::ClockTime {
        self.dts
    }
    pub fn get_width(&self) -> usize {
        self.width
    }
    pub fn get_height(&self) -> usize {
        self.height
    }
    pub fn get_duration(&self) -> gst::ClockTime {
        self.duration
    }
    pub fn get_input_ts(&self) -> f64 {
        self.input_ts
    }
    pub fn get_inference_input(&self) -> &ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>> {
        &self.inference_input
    }
    pub fn get_inference_output(&self) -> &InferenceOutput {
        &self.inference_output
    }
    pub fn set_inference_input(&mut self, input_data: ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>>) {
        self.inference_input = input_data;
    }
    pub fn set_inference_output(&mut self, output_data: InferenceOutput) {
        self.inference_output = output_data;
    }
    pub fn get_pipeline_id(&self) -> &uuid::Uuid {
        &self.pipeline_id
    }
    pub fn set_pipeline_id(&mut self, pipeline_id: &str) {
        self.pipeline_id = uuid::Uuid::from_str(pipeline_id).unwrap();
    }
    pub fn get_user_data(&self) -> &UserData {
        &self.user_data
    }
    pub fn get_frame_number(&self) -> &u64 {
        &self.frame_number
    }
    pub fn set_decibel_value(&mut self, decibel_value: f64) {
        self.decibel_level = decibel_value;
    }
    pub fn get_decibel_value(&self) -> &f64 {
        &self.decibel_level
    }
}

pub enum Frame {
    RgbFrame(RgbFrame)
}

impl Frame {
    pub fn new_rgb(
        original: ndarray::Array3<u8>,
        width: usize, height: usize,
        pts: gst::ClockTime, dts: gst::ClockTime, duration: gst::ClockTime,
        fps: u8, input_ts: f64,
        pipeline_id: uuid::Uuid, frame_number: u64, decibel_level: f64,
    ) -> Self {
        let rgb = RgbFrame::new(
            original, width, height,
            pts, dts, duration,
            fps, input_ts,
            pipeline_id, frame_number, decibel_level,
        );
        Self::RgbFrame(rgb)
    }

    pub fn set_original_pixels(&mut self, original_pixels: ndarray::Array3<u8>) {
        match self {
            Frame::RgbFrame(frame) => { frame.set_original_pixels(original_pixels) }
        }
    }
    pub fn get_original_pixels(&mut self) -> ndarray::ArrayView3<u8> {
        match self {
            Frame::RgbFrame(frame) => frame.get_original_pixels()
        }
    }
    pub fn get_inference_input(&self) -> &ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>> {
        match self {
            Frame::RgbFrame(frame) => frame.get_inference_input()
        }
    }
    pub fn get_inference_output(&self) -> &InferenceOutput {
        match self {
            Frame::RgbFrame(frame) => frame.get_inference_output()
        }
    }
    pub fn set_inference_input(&mut self, input_data: ndarray::ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>>) {
        match self {
            Frame::RgbFrame(frame) => { frame.set_inference_input(input_data); }
        }
    }
    pub fn set_inference_output(&mut self, output_data: InferenceOutput) {
        match self {
            Frame::RgbFrame(frame) => { frame.set_inference_output(output_data); }
        }
    }
    pub fn get_pipeline_id(&self) -> &uuid::Uuid {
        match self {
            Frame::RgbFrame(frame) => frame.get_pipeline_id(),
        }
    }
    pub fn get_frame_number(&self) -> &u64 {
        match self {
            Frame::RgbFrame(frame) => frame.get_frame_number(),
        }
    }
}
