/**
    @file ConnectionSTREAM.cpp
    @author Lime Microsystems
    @brief Implementation of STREAM board connection.
*/

#include "ConnectionXillybusQSpark.h"
#include "ErrorReporting.h"
#ifndef __unix__
#include "Windows.h"
#else
#include <unistd.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include "Si5351C.h"
#include <FPGA_common.h>
#include <LMS7002M.h>
#include <ciso646>

#include <thread>
#include <chrono>

using namespace std;

using namespace lime;


std::mutex ConnectionXillybusQSpark::control_mutex;
/**	@brief Initializes port type and object necessary to communicate to usb device.
*/
ConnectionXillybusQSpark::ConnectionXillybusQSpark(const unsigned index)
{
    RxLoopFunction = bind(&ConnectionXillybusQSpark::ReceivePacketsLoop, this, std::placeholders::_1);
    TxLoopFunction = bind(&ConnectionXillybusQSpark::TransmitPacketsLoop, this, std::placeholders::_1);
    m_hardwareName = "";
    isConnected = false;
    
    endpointIndex = 0;    
#ifndef __unix__
    writeStreamPort = "\\\\.\\xillybus_stream0_write_32";
    readStreamPort = "\\\\.\\xillybus_stream0_read_32";
#else
    writeStreamPort = "/dev/xillybus_stream0_write_32";
    readStreamPort = "/dev/xillybus_stream0_read_32";
#endif
    
#ifndef __unix__
    hWrite = INVALID_HANDLE_VALUE;
    hRead = INVALID_HANDLE_VALUE;
    hWriteStream  = INVALID_HANDLE_VALUE;
    hReadStream  = INVALID_HANDLE_VALUE;
#else
    hWrite = -1;
    hRead = -1;
    hWriteStream = -1;
    hReadStream = -1;
#endif
    if (this->Open(index) != 0)
        std::cerr << GetLastErrorMessage() << std::endl;

    DeviceInfo info = this->GetDeviceInfo();
    std::shared_ptr<Si5351C> si5351module(new Si5351C());
    si5351module->Initialize(this);
    si5351module->SetPLL(0, 25000000, 0);
    si5351module->SetPLL(1, 25000000, 0);
    si5351module->SetClock(0, 27000000, true, false);
    si5351module->SetClock(1, 27000000, true, false);
    for (int i = 2; i < 8; ++i)
        si5351module->SetClock(i, 27000000, false, false);
    Si5351C::Status status = si5351module->ConfigureClocks();
    if (status != Si5351C::SUCCESS)
    {
        std::cerr << "Warning: Failed to configure Si5351C" << std::endl;
        return;
    }
    status = si5351module->UploadConfiguration();
    if (status != Si5351C::SUCCESS)
        std::cerr << "Warning: Failed to upload Si5351C configuration" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); //some settle time 
}

ConnectionXillybusQSpark::ConnectionXillybusQSpark(ConnectionXillybusQSpark &obj)
{
    RxLoopFunction = bind(&ConnectionXillybusQSpark::ReceivePacketsLoop, this, std::placeholders::_1);
    TxLoopFunction = bind(&ConnectionXillybusQSpark::TransmitPacketsLoop, this, std::placeholders::_1);
    m_hardwareName = obj.m_hardwareName;
    isConnected = obj.isConnected;
    hWrite = -1;
    hRead = -1;
    
    endpointIndex = 1;
#ifndef __unix__
    writeStreamPort = "\\\\.\\xillybus_stream1_write_32";
    readStreamPort = "\\\\.\\xillybus_stream1_read_32";
#else
    writeStreamPort = "/dev/xillybus_stream1_write_32";
    readStreamPort = "/dev/xillybus_stream1_read_32";
#endif

#ifndef __unix__
    hWriteStream = INVALID_HANDLE_VALUE;
    hReadStream = INVALID_HANDLE_VALUE;
#else
    hWriteStream = -1;
    hReadStream = -1;
#endif
}

/**	@brief Closes connection to chip and deallocates used memory.
*/
ConnectionXillybusQSpark::~ConnectionXillybusQSpark()
{
    Close();
}

/**	@brief Tries to open connected USB device and find communication endpoints.
	@return Returns 0-Success, other-EndPoints not found or device didn't connect.
*/
int ConnectionXillybusQSpark::Open(const unsigned index)
{
    Close();
    isConnected = true;
    return 0;
}

/**	@brief Closes communication to device.
*/
void ConnectionXillybusQSpark::Close()
{
    isConnected = false;
#ifndef __unix__
	if (hWrite != INVALID_HANDLE_VALUE)
		CloseHandle(hWrite);
	hWrite = INVALID_HANDLE_VALUE;
    if (hRead != INVALID_HANDLE_VALUE)
		CloseHandle(hRead);
	hRead = INVALID_HANDLE_VALUE;

	if (hWriteStream != INVALID_HANDLE_VALUE)
		CloseHandle(hWriteStream);
	if (hReadStream != INVALID_HANDLE_VALUE)
		CloseHandle(hReadStream);
#else
    control_mutex.lock();
    if( hWrite >= 0)
        close(hWrite);
    hWrite = -1;
    if( hRead >= 0)
        close(hRead);
    hRead = -1;
    control_mutex.unlock();
    if( hWriteStream >= 0)
        close(hWriteStream);
    hWriteStream = -1;
    if( hReadStream >= 0)
        close(hReadStream);
    hReadStream = -1;
#endif
}

/**	@brief Returns connection status
	@return 1-connection open, 0-connection closed.
*/
bool ConnectionXillybusQSpark::IsOpen()
{
    return isConnected;
}

int ConnectionXillybusQSpark::TransferPacket(GenericPacket &pkt)
{     
    int timeout_cnt = 100;
    int status = -1;
    control_mutex.lock();
#ifndef __unix__
    const char writePort[] = "\\\\.\\xillybus_control0_write_32";
    const char readPort[] = "\\\\.\\xillybus_control0_read_32";
    while (--timeout_cnt)
    {  
        if ((hWrite = CreateFileA(writePort.c_str(), GENERIC_WRITE, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, 0))!=INVALID_HANDLE_VALUE)
            break;
        Sleep(1);  
    }
    while (timeout_cnt--)
    {  
        if ((hRead = CreateFileA(readPort.c_str(), GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, 0))!=INVALID_HANDLE_VALUE)
            break;
        Sleep(1); 
    }

    if (hWrite != INVALID_HANDLE_VALUE && hRead != INVALID_HANDLE_VALUE)
        status = LMS64CProtocol::TransferPacket(pkt);
    else
        ReportError("Unable to access control port");  
        
    CloseHandle(hWrite);
    CloseHandle(hRead);
#else
    const char writePort[] = "/dev/xillybus_control0_write_32";
    const char readPort[] = "/dev/xillybus_control0_read_32";
    while (--timeout_cnt)
    {
       if ((hWrite = open(writePort, O_WRONLY | O_NOCTTY | O_NONBLOCK))!=-1)
           break;
       usleep(1000);
    }
    while (timeout_cnt--)
    {
       if ((hRead = open(readPort, O_RDONLY | O_NOCTTY | O_NONBLOCK))!=-1)
           break;
       usleep(1000);
    }

    if (hWrite == -1 || hRead ==-1)
        ReportError(errno);   
    else
        status = LMS64CProtocol::TransferPacket(pkt);
    close(hRead);
    close(hWrite);
#endif
    control_mutex.unlock();
    return status;
}

/**	@brief Sends given data buffer to chip through USB port.
	@param buffer data buffer, must not be longer than 64 bytes.
	@param length given buffer size.
    @param timeout_ms timeout limit for operation in milliseconds
	@return number of bytes sent.
*/
int ConnectionXillybusQSpark::Write(const unsigned char *buffer, const int length, int timeout_ms)
{
    long totalBytesWritten = 0;
    long bytesToWrite = length;

#ifndef __unix__
	if (hWrite == INVALID_HANDLE_VALUE)
#else
	if (hWrite == -1)
#endif
        return -1;

    auto t1 = chrono::high_resolution_clock::now();
    auto t2 = chrono::high_resolution_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() < 500)
    {
#ifndef __unix__
		DWORD bytesSent = 0;
		OVERLAPPED	vOverlapped;
		memset(&vOverlapped, 0, sizeof(OVERLAPPED));
		vOverlapped.hEvent = CreateEvent(NULL, false, false, NULL);
        WriteFile(hWrite, buffer + totalBytesWritten, bytesToWrite, &bytesSent, &vOverlapped);
		if (::GetLastError() != ERROR_IO_PENDING)
		{
			CloseHandle(vOverlapped.hEvent);
			return totalBytesWritten;
		}
		std::this_thread::yield();
		DWORD dwRet = WaitForSingleObject(vOverlapped.hEvent, 500);
		if (dwRet == WAIT_OBJECT_0)
		{
			if (GetOverlappedResult(hWrite, &vOverlapped, &bytesSent, FALSE) == FALSE)
			{
				bytesSent = 0;
			}
		}
		else
		{
			CancelIo(hWrite);
			bytesSent = 0;
		}
		CloseHandle(vOverlapped.hEvent);
#else
		int bytesSent;
        if ((bytesSent = write(hWrite, buffer + totalBytesWritten, bytesToWrite))<0)
        {

            if(errno == EINTR)
                 continue;
            else if (errno != EAGAIN)
            {
                ReportError(errno);
                return totalBytesWritten;
            }
        }
		else
#endif
        totalBytesWritten += bytesSent;
        if (totalBytesWritten < length)
        {
            bytesToWrite -= bytesSent;
            t2 = chrono::high_resolution_clock::now();
        }
        else
            break;
    }
#ifdef __unix__
    //Flush data to FPGA
    while (1)
    {
        int rc = write(hWrite, NULL, 0);
        if (rc < 0)
        {
            if (errno == EINTR)
                continue;
            else
            {
                ReportError(errno);
            }
        }
        break;
    }
#endif
    return totalBytesWritten;
}

/**	@brief Reads data coming from the chip through USB port.
	@param buffer pointer to array where received data will be copied, array must be
	big enough to fit received data.
	@param length number of bytes to read from chip.
    @param timeout_ms timeout limit for operation in milliseconds
	@return number of bytes received.
*/
int ConnectionXillybusQSpark::Read(unsigned char *buffer, const int length, int timeout_ms)
{
    memset(buffer, 0, length);
#ifndef __unix__
	if (hRead == INVALID_HANDLE_VALUE)
#else
	if (hRead == -1)
#endif
        {
            return -1;
        }

	long totalBytesReaded = 0;
	long bytesToRead = length;
	auto t1 = chrono::high_resolution_clock::now();
	auto t2 = chrono::high_resolution_clock::now();

	while (std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() < 1000)
	{
 #ifndef __unix__
	   DWORD bytesReceived = 0;
	   OVERLAPPED	vOverlapped;
	   memset(&vOverlapped, 0, sizeof(OVERLAPPED));
	   vOverlapped.hEvent = CreateEvent(NULL, false, false, NULL);
	   ReadFile(hRead, buffer + totalBytesReaded, bytesToRead, &bytesReceived, &vOverlapped);
	   if (::GetLastError() != ERROR_IO_PENDING)
	   {
		 CloseHandle(vOverlapped.hEvent);
		 return totalBytesReaded;
	   }
	   std::this_thread::yield();
	   DWORD dwRet = WaitForSingleObject(vOverlapped.hEvent, 1000);
	   if (dwRet == WAIT_OBJECT_0)
	   {
		   if (GetOverlappedResult(hRead, &vOverlapped, &bytesReceived, TRUE) == FALSE)
		   {
			   bytesReceived = 0;
		   }
	   }
	   else
	   {
		   CancelIo(hRead);
		   bytesReceived = 0;
		}
	   CloseHandle(vOverlapped.hEvent);
#else
            int bytesReceived = 0;
            if ((bytesReceived = read(hRead, buffer+ totalBytesReaded, bytesToRead))<0)
            {
                if(errno == EINTR)
                     continue;
                else if (errno != EAGAIN)
                {
                    ReportError(errno);
                    return totalBytesReaded;
                }
            }
            else
#endif
            totalBytesReaded += bytesReceived;
            
            if (totalBytesReaded < length)
            {
                    bytesToRead -= bytesReceived;
                    t2 = chrono::high_resolution_clock::now();
            }
            else
               break;
        }
    return totalBytesReaded;
}

/**
	@brief Reads data from board
	@param buffer array where to store received data
	@param length number of bytes to read
        @param timeout read timeout in milliseconds
	@return number of bytes received
*/
int ConnectionXillybusQSpark::ReceiveData(char *buffer, uint32_t length, double timeout_ms)
{
    unsigned long totalBytesReaded = 0;
    unsigned long bytesToRead = length;

#ifndef __unix__
    if (hReadStream == INVALID_HANDLE_VALUE)
    {
            hReadStream = CreateFileA(readStreamPort.c_str(), GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, 0);
    }
#else
    if (hReadStream < 0)
    {
       if (( hReadStream = open(readStreamPort.c_str(), O_RDONLY | O_NOCTTY | O_NONBLOCK))<0)
       {
            ReportError(errno);
            return -1;
       }
    }
#endif
    auto t1 = chrono::high_resolution_clock::now();
    auto t2 = chrono::high_resolution_clock::now();

    while (std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() < timeout_ms)
    {
 #ifndef __unix__
		DWORD bytesReceived = 0;
		OVERLAPPED	vOverlapped;
		memset(&vOverlapped, 0, sizeof(OVERLAPPED));
		vOverlapped.hEvent = CreateEvent(NULL, false, false, NULL);
		ReadFile(hReadStream, buffer + totalBytesReaded, bytesToRead, &bytesReceived, &vOverlapped);
		if (::GetLastError() != ERROR_IO_PENDING)
		{
			CloseHandle(vOverlapped.hEvent);
			return totalBytesReaded;
		}
		DWORD dwRet = WaitForSingleObject(vOverlapped.hEvent, timeout_ms);
		if (dwRet == WAIT_OBJECT_0)
		{
			if (GetOverlappedResult(hReadStream, &vOverlapped, &bytesReceived, TRUE) == FALSE)
			{
				bytesReceived = 0;
			}
		}
		else
		{
			CancelIo(hReadStream);
			bytesReceived = 0;
		}
		CloseHandle(vOverlapped.hEvent);
#else
		int bytesReceived = 0;
        if ((bytesReceived = read(hReadStream, buffer+ totalBytesReaded, bytesToRead))<0)
        {
            bytesReceived = 0;
            if(errno == EINTR)
                 continue;
            else if (errno != EAGAIN)
            {
                ReportError(errno);
                return totalBytesReaded;
            }
        }
#endif
        totalBytesReaded += bytesReceived;
        if (totalBytesReaded < length)
        {
            bytesToRead -= bytesReceived;
            t2 = chrono::high_resolution_clock::now();
        }
        else
            break;
    }

    return totalBytesReaded;
}

/**
	@brief Aborts reading operations
*/
void ConnectionXillybusQSpark::AbortReading()
{
#ifndef __unix__
    if (hReadStream != INVALID_HANDLE_VALUE)
    {
        CloseHandle(hReadStream);
		hReadStream = INVALID_HANDLE_VALUE;
    }
#else
    if (hReadStream >= 0)
    {
        close(hReadStream);
        hReadStream =-1;
    }
#endif
}

int ConnectionXillybusQSpark::UploadWFM(const void* const* samples, uint8_t chCount, size_t sample_count, StreamConfig::StreamDataFormat format)
{
    WriteRegister(0x000C, chCount == 1 ? 0x1 : 0x3); //channels 0,1
    WriteRegister(0x000E, 0x2); //12bit samples
    WriteRegister(0x000D, 0x0004); //WFM_LOAD

    lime::FPGA_DataPacket pkt;
    size_t samplesUsed = 0;

    const complex16_t* const* src = (const complex16_t* const*)samples;
    int cnt = sample_count;

    const lime::complex16_t** batch = new const lime::complex16_t*[chCount];
    while (cnt > 0)
    {
        pkt.counter = 0;
        pkt.reserved[0] = 0;
        int samplesToSend = cnt > 1360 / chCount ? 1360 / chCount : cnt;
        cnt -= samplesToSend;

        for (uint8_t i = 0; i<chCount; ++i)
            batch[i] = &src[i][samplesUsed];
        samplesUsed += samplesToSend;

        size_t bufPos = 0;
        lime::fpga::Samples2FPGAPacketPayload(batch, samplesToSend, chCount, format, pkt.data, &bufPos);
        int payloadSize = (bufPos / 4) * 4;
        if (bufPos % 4 != 0)
            printf("Packet samples count not multiple of 4\n");
        pkt.reserved[2] = (payloadSize >> 8) & 0xFF; //WFM loading
        pkt.reserved[1] = payloadSize & 0xFF; //WFM loading
        pkt.reserved[0] = 0x1 << 5; //WFM loading

        long bToSend = 16 + payloadSize;
        SendData((const char*)&pkt, bToSend, 1000);
    }
    delete[] batch;
#ifndef __unix__
    Sleep(1000);
#else
    sleep(1);
#endif
    AbortSending();
    if (cnt == 0)
        return 0;
    else
        return ReportError(-1, "Failed to upload waveform");
}

/**
	@brief  sends data to board
	@param *buffer buffer to send
	@param length number of bytes to send
        @param timeout data write timeout in milliseconds
	@return number of bytes sent
*/
int ConnectionXillybusQSpark::SendData(const char *buffer, uint32_t length, double timeout_ms)
{
#ifndef __unix__
	if (hWriteStream == INVALID_HANDLE_VALUE)
	{
        hWriteStream = CreateFileA(writeStreamPort.c_str(), GENERIC_WRITE, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, 0);
	}
#else
        if (hWriteStream < 0)
        {
           if ((hWriteStream = open(writeStreamPort.c_str(), O_WRONLY | O_NOCTTY | O_NONBLOCK))<0)
           {
                ReportError(errno);
		return -1;
           }
        }

#endif
    unsigned long totalBytesWritten = 0;
    unsigned long bytesToWrite = length;
    auto t1 = chrono::high_resolution_clock::now();
    auto t2 = chrono::high_resolution_clock::now();

    while (std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() < timeout_ms)
    {
#ifndef __unix__
		DWORD bytesSent = 0;
		OVERLAPPED	vOverlapped;
		memset(&vOverlapped, 0, sizeof(OVERLAPPED));
		vOverlapped.hEvent = CreateEvent(NULL, false, false, NULL);
		WriteFile(hWriteStream, buffer + totalBytesWritten, bytesToWrite, &bytesSent, &vOverlapped);
		if (::GetLastError() != ERROR_IO_PENDING)
		{
			CloseHandle(vOverlapped.hEvent);
			return totalBytesWritten;
		}
		DWORD dwRet = WaitForSingleObject(vOverlapped.hEvent, timeout_ms);
		if (dwRet == WAIT_OBJECT_0)
		{
			if (GetOverlappedResult(hWriteStream, &vOverlapped, &bytesSent, TRUE) == FALSE)
			{
				bytesSent = 0;
			}
		}
		else
		{
			CancelIo(hWriteStream);
			bytesSent = 0;
		}
		CloseHandle(vOverlapped.hEvent);
#else
		int bytesSent = 0;
        if ((bytesSent  = write(hWriteStream, buffer+ totalBytesWritten, bytesToWrite))<0)
        {
            bytesSent =0;
            if(errno == EINTR)
                 continue;
            else if (errno != EAGAIN)
            {
                ReportError(errno);
                return totalBytesWritten;
            }
        }
#endif
        totalBytesWritten += bytesSent;
        if (totalBytesWritten < length)
        {
            bytesToWrite -= bytesSent;
            t2 = chrono::high_resolution_clock::now();
        }
        else
            break;
    }
    //Flush data to FPGA
#ifdef __unix__
    while (1)
    {
        int rc = write(hWriteStream, NULL, 0);
        if (rc < 0)
        {
            if (errno == EINTR)
                continue;
            else
            {
                ReportError(errno);
            }
        }
        break;
    }
#else
    FlushFileBuffers(hWriteStream);
#endif 
    return totalBytesWritten;
}

/**
	@brief Aborts sending operations
*/
void ConnectionXillybusQSpark::AbortSending()
{
#ifndef __unix__
    if (hWriteStream != INVALID_HANDLE_VALUE)
    {
        CloseHandle(hWriteStream);
		hWriteStream = INVALID_HANDLE_VALUE;
    }
#else
    if (hWriteStream >= 0)
    {
        close (hWriteStream);
        hWriteStream = -1;
    }
#endif
}


/** @brief Configures FPGA PLLs to LimeLight interface frequency
*/
int ConnectionXillybusQSpark::UpdateExternalDataRate(const size_t channel, const double txRate_Hz, const double rxRate_Hz)
{
    std::cout << "ConnectionXillybus::ConfigureFPGA_PLL(tx=" << txRate_Hz/1e6 << "MHz, rx=" << rxRate_Hz/1e6 << "MHz)" << std::endl;
    const float txInterfaceClk = 2 * txRate_Hz;
    const float rxInterfaceClk = 2 * rxRate_Hz;
    mExpectedSampleRate = rxRate_Hz;
    int status = 0;
    int pll_ind = (channel == 1) ? 2 : 0;
    if(txInterfaceClk >= 5e6)
    {
        lime::fpga::FPGA_PLL_clock clocks[2];
        clocks[0].bypass = false;
        clocks[0].index = 0;
        clocks[0].outFrequency = txInterfaceClk;
        clocks[0].phaseShift_deg = 0;
        clocks[1].bypass = false;
        clocks[1].index = 1;
        clocks[1].outFrequency = txInterfaceClk;
        clocks[1].phaseShift_deg = 90;
        status = lime::fpga::SetPllFrequency(this, pll_ind, txInterfaceClk, clocks, 2);
    }
    else
        status = lime::fpga::SetDirectClocking(this, pll_ind, txInterfaceClk, 90);
    if(status != 0)
        return status;

    if(rxInterfaceClk >= 5e6)
    {
        lime::fpga::FPGA_PLL_clock clocks[2];
        clocks[0].bypass = false;
        clocks[0].index = 0;
        clocks[0].outFrequency = rxInterfaceClk;
        clocks[0].phaseShift_deg = 0;
        clocks[1].bypass = false;
        clocks[1].index = 1;
        clocks[1].outFrequency = rxInterfaceClk;
        clocks[1].phaseShift_deg = 90;
        status = lime::fpga::SetPllFrequency(this, pll_ind+1, rxInterfaceClk, clocks, 2);
    }
    else
        status = lime::fpga::SetDirectClocking(this, pll_ind+1, rxInterfaceClk, 90);
    return status;
}

/** @brief Function dedicated for receiving data samples from board
    @param rxFIFO FIFO to store received data
    @param terminate periodically pooled flag to terminate thread
    @param dataRate_Bps (optional) if not NULL periodically returns data rate in bytes per second
*/
void ConnectionXillybusQSpark::ReceivePacketsLoop(const ThreadData args)
{
    auto terminate = args.terminate;
    auto dataRate_Bps = args.dataRate_Bps;
    auto generateData = args.generateData;
    auto safeToConfigInterface = args.safeToConfigInterface;

    //at this point FPGA has to be already configured to output samples
    const uint8_t chCount = args.channels.size();
    const auto link = args.channels[0]->config.linkFormat;
    const uint32_t samplesInPacket = (link == StreamConfig::STREAM_12_BIT_COMPRESSED ? 1360 : 1020)/chCount;

    double latency=0;
    for (int i = 0; i < chCount; i++)
    {
        latency += args.channels[i]->config.performanceLatency/chCount;
    }
    const unsigned tmp_cnt = (latency * 6)+0.5;

    const uint8_t packetsToBatch = (1<<tmp_cnt);
    const uint32_t bufferSize = packetsToBatch*sizeof(FPGA_DataPacket);
    vector<char>buffers(bufferSize, 0);
    vector<StreamChannel::Frame> chFrames;
    try
    {
        chFrames.resize(chCount);
    }
    catch (const std::bad_alloc &ex)
    {
        ReportError("Error allocating Rx buffers, not enough memory");
        return;
    }

    unsigned long totalBytesReceived = 0; //for data rate calculation
    int m_bufferFailures = 0;
    int32_t droppedSamples = 0;
    int32_t packetLoss = 0;

    vector<uint32_t> samplesReceived(chCount, 0);

    auto t1 = chrono::high_resolution_clock::now();
    auto t2 = chrono::high_resolution_clock::now();

    std::mutex txFlagsLock;
    condition_variable resetTxFlags;
    //worker thread for reseting late Tx packet flags
    std::thread txReset([](ILimeSDRStreaming* port,
                        atomic<bool> *terminate,
                        mutex *spiLock,
                        condition_variable *doWork)
    {
        uint32_t reg9;
        port->ReadRegister(0x0009, reg9);
        const uint32_t addr[] = {0x0009, 0x0009};
        const uint32_t data[] = {reg9 | (0xA), reg9 & ~(0xA)};
        while (not terminate->load())
        {
            std::unique_lock<std::mutex> lck(*spiLock);
            doWork->wait(lck);
            port->WriteRegisters(addr, data, 2);
        }
    }, this, terminate, &txFlagsLock, &resetTxFlags);

    int resetFlagsDelay = 128;
    uint64_t prevTs = 0;
    while (terminate->load() == false)
    {
        if(generateData->load())
        {
            fpga::StopStreaming(this);
            safeToConfigInterface->notify_all(); //notify that it's safe to change chip config
            const int batchSize = (this->mExpectedSampleRate/chFrames[0].samplesCount)/10;
            IStreamChannel::Metadata meta;
            for(int i=0; i<batchSize; ++i)
            {
                for(int ch=0; ch<chCount; ++ch)
                {
                    meta.timestamp = chFrames[ch].timestamp;
                    for(int j=0; j<chFrames[ch].samplesCount; ++j)
                    {
                        chFrames[ch].samples[j].i = 0;
                        chFrames[ch].samples[j].q = 0;
                    }
                    uint32_t samplesPushed = args.channels[ch]->Write((const void*)chFrames[ch].samples, chFrames[ch].samplesCount, &meta);
                    samplesReceived[ch] += chFrames[ch].samplesCount;
                    if(samplesPushed != chFrames[ch].samplesCount)
                        printf("Rx samples pushed %i/%i\n", samplesPushed, chFrames[ch].samplesCount);
                }
            }
            this_thread::sleep_for(chrono::milliseconds(100));
        }
        int32_t bytesReceived = 0;

        bytesReceived = this->ReceiveData(&buffers[0], bufferSize,200);
        if (bytesReceived == bufferSize)
        {
            totalBytesReceived += bytesReceived;
            if (bytesReceived != int32_t(bufferSize)) //data should come in full sized packets
                ++m_bufferFailures;

            bool txLate=false;
            for (uint8_t pktIndex = 0; pktIndex < bytesReceived / sizeof(FPGA_DataPacket); ++pktIndex)
            {
                const FPGA_DataPacket* pkt = (FPGA_DataPacket*)&buffers[0];
                const uint8_t byte0 = pkt[pktIndex].reserved[0];
                if ((byte0 & (1 << 3)) != 0 && !txLate) //report only once per batch
                {
                    txLate = true;
                    if(resetFlagsDelay > 0)
                        --resetFlagsDelay;
                    else
                    {
                        printf("L");
                        resetTxFlags.notify_one();
                        resetFlagsDelay = packetsToBatch*2;
                    }
                }
                uint8_t* pktStart = (uint8_t*)pkt[pktIndex].data;
                if(pkt[pktIndex].counter - prevTs != samplesInPacket && pkt[pktIndex].counter != prevTs)
                {
    #ifndef NDEBUG
                    printf("\tRx pktLoss@%i - ts diff: %li  pktLoss: %.1f\n", pktIndex, pkt[pktIndex].counter - prevTs, float(pkt[pktIndex].counter - prevTs)/samplesInPacket);
    #endif
                    packetLoss += (pkt[pktIndex].counter - prevTs)/samplesInPacket;
                }
                prevTs = pkt[pktIndex].counter;
                if(args.lastTimestamp)
                    args.lastTimestamp->store(pkt[pktIndex].counter);
                //parse samples
                vector<complex16_t*> dest(chCount);
                for(uint8_t c=0; c<chCount; ++c)
                    dest[c] = (chFrames[c].samples);
                size_t samplesCount = 0;
                fpga::FPGAPacketPayload2Samples(pktStart, 4080, chCount, link, dest.data(), &samplesCount);

                for(int ch=0; ch<chCount; ++ch)
                {
                    IStreamChannel::Metadata meta;
                    meta.timestamp = pkt[pktIndex].counter;
                    meta.flags = RingFIFO::OVERWRITE_OLD;
                    uint32_t samplesPushed = args.channels[ch]->Write((const void*)chFrames[ch].samples, samplesCount, &meta, 100);
                    if(samplesPushed != samplesCount)
                        droppedSamples += samplesCount-samplesPushed;
                }
            }
        }
        // Re-submit this request to keep the queue full
        if(not generateData->load())
        {
            //fpga::StartStreaming(this);
        }
        t2 = chrono::high_resolution_clock::now();
        auto timePeriod = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
        if (timePeriod >= 1000)
        {
            t1 = t2;
            //total number of bytes sent per second
            double dataRate = 1000.0*totalBytesReceived / timePeriod;
            //each channel sample rate
            float samplingRate = 1000.0*samplesReceived[0] / timePeriod;
#ifndef NDEBUG
            printf("Rx: %.3f MB/s, Fs: %.3f MHz, overrun: %i, loss: %i \n", dataRate / 1000000.0, samplingRate / 1000000.0, droppedSamples, packetLoss);
#endif
            samplesReceived[0] = 0;
            totalBytesReceived = 0;
            m_bufferFailures = 0;
            droppedSamples = 0;
            packetLoss = 0;

            if (dataRate_Bps)
                dataRate_Bps->store((uint32_t)dataRate);
        }
    }
    this->AbortReading();
    resetTxFlags.notify_one();
    txReset.join();
    if (dataRate_Bps)
        dataRate_Bps->store(0);
}

/** @brief Functions dedicated for transmitting packets to board
    @param txFIFO data source FIFO
    @param terminate periodically pooled flag to terminate thread
    @param dataRate_Bps (optional) if not NULL periodically returns data rate in bytes per second
*/
void ConnectionXillybusQSpark::TransmitPacketsLoop(const ThreadData args)
{
    auto terminate = args.terminate;
    auto dataRate_Bps = args.dataRate_Bps;

    //at this point FPGA has to be already configured to output samples
    const uint8_t maxChannelCount = 2;
    const uint8_t chCount = args.channels.size();
    const auto link = args.channels[0]->config.linkFormat;

    double latency=0;
    for (int i = 0; i < chCount; i++)
    {
        latency += args.channels[i]->config.performanceLatency/chCount;
    }
    const unsigned tmp_cnt = (latency * 6)+0.5;
    const uint8_t packetsToBatch = (1<<tmp_cnt); //packets in single USB transfer
    const uint32_t bufferSize = packetsToBatch*4096;
    const uint32_t popTimeout_ms = 100;

    const int maxSamplesBatch = (link==StreamConfig::STREAM_12_BIT_COMPRESSED?1360:1020)/chCount;
    vector<complex16_t> samples[maxChannelCount];
    vector<char> buffers;
    try
    {
        for(int i=0; i<chCount; ++i)
            samples[i].resize(maxSamplesBatch);
        buffers.resize(bufferSize, 0);
    }
    catch (const std::bad_alloc& ex) //not enough memory for buffers
    {
        printf("Error allocating Tx buffers, not enough memory\n");
        return;
    }

    int m_bufferFailures = 0;
    long totalBytesSent = 0;

    uint32_t samplesSent = 0;

    auto t1 = chrono::high_resolution_clock::now();
    auto t2 = chrono::high_resolution_clock::now();

    uint64_t timestamp = 0;
    while (terminate->load() != true)
    {
        int i=0;

        while(i<packetsToBatch)
        {
            IStreamChannel::Metadata meta;
            FPGA_DataPacket* pkt = reinterpret_cast<FPGA_DataPacket*>(&buffers[0]);
            for(int ch=0; ch<chCount; ++ch)
            {
                int samplesPopped = args.channels[ch]->Read(samples[ch].data(), maxSamplesBatch, &meta, popTimeout_ms);
                if (samplesPopped != maxSamplesBatch)
                {
                #ifndef NDEBUG
                    printf("Warning popping from TX, samples popped %i/%i\n", samplesPopped, maxSamplesBatch);
                #endif
                }
            }
            if(terminate->load() == true) //early termination
                break;
            pkt[i].counter = meta.timestamp;
            pkt[i].reserved[0] = 0;
            //by default ignore timestamps
            const int ignoreTimestamp = !(meta.flags & IStreamChannel::Metadata::SYNC_TIMESTAMP);
            pkt[i].reserved[0] |= ((int)ignoreTimestamp << 4); //ignore timestamp

            vector<complex16_t*> src(chCount);
            for(uint8_t c=0; c<chCount; ++c)
                src[c] = (samples[c].data());
            uint8_t* const dataStart = (uint8_t*)pkt[i].data;
            fpga::Samples2FPGAPacketPayload(src.data(), maxSamplesBatch, chCount, link, dataStart, nullptr);
            samplesSent += maxSamplesBatch;
            ++i;
        }

        uint32_t bytesSent = this->SendData(&buffers[0], bufferSize,200);
                totalBytesSent += bytesSent;
        if (bytesSent != bufferSize)
            ++m_bufferFailures;

        t2 = chrono::high_resolution_clock::now();
        auto timePeriod = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
        if (timePeriod >= 1000)
        {
            //total number of bytes sent per second
            float dataRate = 1000.0*totalBytesSent / timePeriod;
            //total number of samples from all channels per second
            float sampleRate = 1000.0*samplesSent / timePeriod;
            if(dataRate_Bps)
                dataRate_Bps->store(dataRate);
            m_bufferFailures = 0;
            samplesSent = 0;
            totalBytesSent = 0;
            t1 = t2;
#ifndef NDEBUG
            printf("Tx: %.3f MB/s, Fs: %.3f MHz, failures: %i, ts:%li\n", dataRate / 1000000.0, sampleRate / 1000000.0, m_bufferFailures, timestamp);
#endif
        }
    }

    // Wait for all the queued requests to be cancelled
    this->AbortSending();
    if (dataRate_Bps)
        dataRate_Bps->store(0);
}

int ConnectionXillybusQSpark::UpdateThreads()
{
    bool needTx = false;
    bool needRx = false;
    //check which threads are needed
    for(auto i : mRxStreams)
        if(i->IsActive())
        {
            needRx = true;
            break;
        }
    for(auto i : mTxStreams)
        if(i->IsActive())
        {
            needTx = true;
            break;
        }

    //stop threads if not needed
    if(not needTx and txRunning.load())
    {
        terminateTx.store(true);
        txThread.join();
        txRunning.store(false);
    }
    if(not needRx and rxRunning.load())
    {
        terminateRx.store(true);
        rxThread.join();
        rxRunning.store(false);
    }

    //configure FPGA on first start, or disable FPGA when not streaming
    if((needTx or needRx) && (not rxRunning.load() and not txRunning.load()))
    {
        //enable FPGA streaming
        fpga::StopStreaming(this,endpointIndex);
        fpga::ResetTimestamp(this,endpointIndex);
        //USB FIFO reset
        // TODO : USB FIFO reset command for IConnection
        LMS64CProtocol::GenericPacket ctrPkt;
        ctrPkt.cmd = CMD_USB_FIFO_RST;
        ctrPkt.outBuffer.push_back(0x00);
        TransferPacket(ctrPkt);
        
        //enable MIMO mode, 12 bit compressed values
        StreamConfig config;
        config.linkFormat = StreamConfig::STREAM_12_BIT_COMPRESSED;
        //by default use 12 bit compressed, adjust link format for stream

        for(auto i : mRxStreams)
        {
            if(i->config.format == StreamConfig::STREAM_12_BIT_IN_16)
            {
                config.linkFormat = StreamConfig::STREAM_12_BIT_IN_16;
                break;
            }
        }
        for(auto i : mTxStreams)
        {
            if(i->config.format == StreamConfig::STREAM_12_BIT_IN_16)
            {
                config.linkFormat = StreamConfig::STREAM_12_BIT_IN_16;
                break;
            }
        }
        for(auto i : mRxStreams)
            i->config.linkFormat = config.linkFormat;
        for(auto i : mTxStreams)
            i->config.linkFormat = config.linkFormat;

        uint16_t smpl_width; // 0-16 bit, 1-14 bit, 2-12 bit
        if(config.linkFormat == StreamConfig::STREAM_12_BIT_IN_16)
            smpl_width = 0x0;
        else if(config.linkFormat == StreamConfig::STREAM_12_BIT_COMPRESSED)
            smpl_width = 0x2;
        else
            smpl_width = 0x2;
        WriteRegister(0x0008, 0x0100 | smpl_width);

        uint16_t channelEnables = 0;
        for(uint8_t i=0; i<mRxStreams.size(); ++i)
            channelEnables |= (1 << (mRxStreams[i]->config.channelID&0x1));
        for(uint8_t i=0; i<mTxStreams.size(); ++i)
            channelEnables |= (1 << (mTxStreams[i]->config.channelID&0x1));
        WriteRegister(0x0007, channelEnables);

        LMS7002M lmsControl;
        lmsControl.SetConnection(this,endpointIndex);
        bool fromChip = true;
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML1_MODE), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML2_MODE), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML1_FIDM), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML2_FIDM), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(PD_RX_AFE1), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(PD_TX_AFE1), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(PD_RX_AFE2), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(PD_TX_AFE2), 0, fromChip);

        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML2_S0S), 1, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML2_S1S), 0, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML2_S2S), 3, fromChip);
        lmsControl.Modify_SPI_Reg_bits(LMS7param(LML2_S3S), 2, fromChip);

        if(channelEnables & 0x2) //enable MIMO
        {
            uint16_t macBck = lmsControl.Get_SPI_Reg_bits(LMS7param(MAC), fromChip);
            lmsControl.Modify_SPI_Reg_bits(LMS7param(MAC), 1, fromChip);
            lmsControl.Modify_SPI_Reg_bits(LMS7param(EN_NEXTRX_RFE), 1, fromChip);
            lmsControl.Modify_SPI_Reg_bits(LMS7param(EN_NEXTTX_TRF), 1, fromChip);
            lmsControl.Modify_SPI_Reg_bits(LMS7param(MAC), macBck, fromChip);
        }
        fpga::StartStreaming(this,endpointIndex);
    }
    else if(not needTx and not needRx)
    {   
        //disable FPGA streaming
        fpga::StopStreaming(this,endpointIndex);
    }

    //FPGA should be configured and activated, start needed threads
    if(needRx and not rxRunning.load())
    {
        ThreadData args;
        args.terminate = &terminateRx;
        args.dataPort = this;
        args.dataRate_Bps = &rxDataRate_Bps;
        args.channels = mRxStreams;
        args.generateData = &generateData;
        args.safeToConfigInterface = &safeToConfigInterface;
        args.lastTimestamp = &rxLastTimestamp;;

        rxRunning.store(true);
        terminateRx.store(false);
        rxThread = std::thread(RxLoopFunction, args);
    }
    if(needTx and not txRunning.load())
    {
        ThreadData args;
        args.terminate = &terminateTx;
        args.dataPort = this;
        args.dataRate_Bps = &txDataRate_Bps;
        args.channels = mTxStreams;
        args.generateData = &generateData;
        args.safeToConfigInterface = &safeToConfigInterface;
        args.lastTimestamp = nullptr;

        txRunning.store(true);
        terminateTx.store(false);
        txThread = std::thread(TxLoopFunction, args);
    }
    return 0;
}

int ConnectionXillybusQSpark::ReadRawBuffer(char* buffer, unsigned length)
{
    //fpga::StopStreaming(this);
    //fpga::ResetTimestamp(this);
    //USB FIFO reset
    // TODO : USB FIFO reset command for IConnection
    LMS64CProtocol::GenericPacket ctrPkt;
    ctrPkt.cmd = CMD_USB_FIFO_RST;
    ctrPkt.outBuffer.push_back(0x00);
    TransferPacket(ctrPkt);
    //fpga::StartStreaming(this);

    vector<uint32_t> addrs;
    vector<uint32_t> values;
    addrs.push_back(0x0040); values.push_back(length/12);
    addrs.push_back(0x0041); values.push_back(0);
    this->WriteRegisters(addrs.data(), values.data(), values.size());
    addrs.clear(); values.clear();
    addrs.push_back(0x0041); values.push_back(1);
    this->WriteRegisters(addrs.data(), values.data(), values.size());

    int ret = ReceiveData(buffer, length, 1000);

    AbortReading();
    //fpga::StopStreaming(this);
    return ret;
}

DeviceInfo ConnectionXillybusQSpark::GetDeviceInfo(void)
{
    DeviceInfo info = LMS64CProtocol::GetDeviceInfo();
    info.addrsLMS7002M.push_back(info.addrsLMS7002M[0]+1);
    return info;
}
