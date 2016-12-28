/**
    @file ConnectionSTREAM.h
    @author Lime Microsystems
    @brief Implementation of STREAM board connection.
*/

#pragma once
#include <ConnectionRegistry.h>
#include <ILimeSDRStreaming.h>
#include "ConnectionXillybus.h"
#include <vector>
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include "fifo.h"

#ifndef __unix__
#include "windows.h"
#else
#include <mutex>
#include <condition_variable>
#include <chrono>
#endif

namespace lime{

class ConnectionXillybusQSpark : public ILimeSDRStreaming
{
public:
    ConnectionXillybusQSpark(const unsigned index);
    ConnectionXillybusQSpark(ConnectionXillybusQSpark &obj);
    ~ConnectionXillybusQSpark(void);

	int Open(const unsigned index);
	void Close();
	bool IsOpen();
	int GetOpenedIndex();

	int Write(const unsigned char *buffer, int length, int timeout_ms = 100) override;
	int Read(unsigned char *buffer, int length, int timeout_ms = 100) override;
        int TransferPacket(GenericPacket &pkt)override;

	//hooks to update FPGA plls when baseband interface data rate is changed
	int UpdateExternalDataRate(const size_t channel, const double txRate, const double rxRate) override;

        int ReadRawBuffer(char* buffer, unsigned length) override;
        int UpdateThreads() override;
        int UploadWFM(const void* const* samples, uint8_t chCount, size_t sample_count, StreamConfig::StreamDataFormat format) override;
        DeviceInfo GetDeviceInfo(void)override;
protected:
    void ReceivePacketsLoop(const ThreadData args) override;
    void TransmitPacketsLoop(const ThreadData args) override;

    int ReceiveData(char* buffer, uint32_t length, double timeout);
    void AbortReading();

    int SendData(const char* buffer, uint32_t length, double timeout);
    void AbortSending();
private:
    eConnectionType GetType(void)
    {
        return PCIE_PORT;
    }

    std::string m_hardwareName;
    int m_hardwareVer;

    bool isConnected;

#ifndef __unix__
    HANDLE hWrite;
    HANDLE hRead;
    HANDLE hWriteStream;
    HANDLE hReadStream;
#else
    int hWrite;
    int hRead;
    int hWriteStream;
    int hReadStream;
#endif
    std::string writeStreamPort;
    std::string readStreamPort;
    unsigned endpointIndex;
    static std::mutex control_mutex;
};




}
