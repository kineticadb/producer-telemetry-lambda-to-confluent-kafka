import os
import json

from datetime import timezone
from time import strftime

from kafka import KafkaProducer

KAFKA_BRKR=os.getenv('KAFKA_HOST')
KAFKA_PORT=os.getenv('KAFKA_PORT')
KAFKA_USER=os.getenv('KAFKA_SASL_USER')
KAFKA_PASS=os.getenv('KAFKA_SASL_PASS')
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC')

def main(event, context):   
    lambda_handler(event, context)

def lambda_handler(event, context):
    producer = KafkaProducer(bootstrap_servers=[f"{KAFKA_BRKR}:{KAFKA_PORT}"],
        sasl_plain_username = KAFKA_USER,
        sasl_plain_password = KAFKA_PASS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        value_serializer=lambda x:
        json.dumps(x).encode('utf-8'))
    producer.send(KAFKA_TOPIC, value=json.loads(event["body"]))
    producer.flush()

    e = event
    e["body"]=json.loads(event["body"])
    return { 
        'message' : e
    }

if __name__ == "__main__":   
    payload = {}
    payload["body"] = {
            "accelerometerAccelerationX": "0.002167",
            "accelerometerAccelerationY": "-0.008240",
            "accelerometerAccelerationZ": "-0.998581",
            "accelerometerTimestamp_sinceReboot": "178223.651375",
            "activity": "stationary",
            "activityActivityConfidence": "2",
            "activityActivityStartDate": "2021-10-21 20:41:47.683 -0400",
            "activityTimestamp_sinceReboot": "178104.863461",
            "altimeterPressure": "100.398735",
            "altimeterRelativeAltitude": "0.409561",
            "altimeterReset": "0",
            "altimeterTimestamp_sinceReboot": "178221.067158",
            "avAudioRecorder_Timestamp_since1970": "1634863426.440878",
            "avAudioRecorderAveragePower": "-19.862366",
            "avAudioRecorderPeakPower": "-17.500328",
            "batteryLevel": "0.330000",
            "batteryState": "1",
            "batteryTimeStamp_since1970": "1634863329.092033",
            "deviceID": "my_iOS_device",
            "deviceOrientation": "5",
            "deviceOrientationTimeStamp_since1970": "1634863172.283096",
            "gyroRotationX": "-0.011401",
            "gyroRotationY": "0.007669",
            "gyroRotationZ": "0.007558",
            "gyroTimestamp_sinceReboot": "178223.738465",
            "identifierForVendor": "044AD7D6-AEEB-4159-B132-022BD5A6F8E8",
            "IP_en0": "192.168.1.152",
            "IP_pdp_ip0": "100.82.240.239",
            "IP_Timestamp_since1970": "1634863165.660642",
            "label": "0",
            "locationAltitude": "59.377625",
            "locationCourse": "113.103267",
            "locationFloor": "-9999",
            "locationHeadingAccuracy": "13.105134",
            "locationHeadingTimestamp_since1970": "1634863426.998747",
            "locationHeadingX": "6.116943",
            "locationHeadingY": "-17.734177",
            "locationHeadingZ": "-46.231308",
            "locationHorizontalAccuracy": "5.076879",
            "locationLatitude": "40.300030",
            "locationLongitude": "-74.758929",
            "locationMagneticHeading": "200.161484",
            "locationSpeed": "0.000000",
            "locationTimestamp_since1970": "1634863425.999642",
            "locationTrueHeading": "187.936935",
            "locationVerticalAccuracy": "2.837224",
            "loggingTime": "2021-10-21 20:43:47.013 -0400",
            "magnetometerTimestamp_sinceReboot": "178224.220424",
            "magnetometerX": "-302.222046",
            "magnetometerY": "-4.388794",
            "magnetometerZ": "-78.261581",
            "motionAttitudeReferenceFrame": "XArbitraryZVertical",
            "motionGravityX": "0.002421",
            "motionGravityY": "-0.008922",
            "motionGravityZ": "-0.999957",
            "motionHeading": "-1.000000",
            "motionMagneticFieldCalibrationAccuracy": "-1.000000",
            "motionMagneticFieldX": "0.000000",
            "motionMagneticFieldY": "0.000000",
            "motionMagneticFieldZ": "0.000000",
            "motionPitch": "0.008922",
            "motionQuaternionW": "-0.993130",
            "motionQuaternionX": "-0.004572",
            "motionQuaternionY": "-0.000681",
            "motionQuaternionZ": "0.116929",
            "motionRoll": "0.002422",
            "motionRotationRateX": "0.001212",
            "motionRotationRateY": "0.000769",
            "motionRotationRateZ": "0.000148",
            "motionTimestamp_sinceReboot": "178223.266936",
            "motionUserAccelerationX": "-0.000133",
            "motionUserAccelerationY": "0.000835",
            "motionUserAccelerationZ": "0.002353",
            "motionYaw": "-0.234408",
            "pedometerAverageActivePace": "0.000000",
            "pedometerCurrentCadence": "0.000000",
            "pedometerCurrentPace": "0.000000",
            "pedometerDistance": "0.000000",
            "pedometerEndDate": "null",
            "pedometerFloorsAscended": "0",
            "pedometerFloorsDescended": "0",
            "pedometerNumberOfSteps": "0",
            "pedometerStartDate": "null"
        }
    main(event=payload, context={})